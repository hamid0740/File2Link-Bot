# Modules & Libraries
import boto3
import yaml
import datetime
import jdatetime
import dateutil
import os
import asyncio
import urllib.parse
# Enable logging
import logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO, datefmt="%Y/%m/%d %H:%M:%S")
logger = logging.getLogger(__name__)
# Telegram
from pyrogram import Client, filters, enums
from pyrogram.types import Message
# Defining global values
config = yaml.safe_load(open("config.yml", "r", encoding="utf-8"))
messages = yaml.safe_load(open("messages.yml", "r", encoding="utf-8"))

# Defining pyrogram app
app = Client(
  name=config["bot_username"],
  api_id=config["tg_api_id"],
  api_hash=config["tg_api_hash"],
  bot_token=config["tg_bot_token"]
)

# Defining bucket
s3_resource = boto3.resource(
  "s3",
  endpoint_url=config["s3_endpoint_url"],
  aws_access_key_id=config["s3_access_key"],
  aws_secret_access_key=config["s3_secret_key"]
)
bucket = s3_resource.Bucket(config["s3_bucket_name"])

# Function: Make file size human-readable
def humanize(num, suffix="B"):
  for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
    if abs(num) < 1024.0:
      return f"{num:3.1f}{unit}{suffix}"
    num /= 1024.0
  return f"{num:.1f}Yi{suffix}"

# Function: Localize time
def time_localize(dt):
  from_zone = dateutil.tz.tzutc()
  to_zone = dateutil.tz.gettz(config["timezone"])
  dt = dt.replace(tzinfo=from_zone).astimezone(to_zone)
  if config["use_jalali_date"]:
    dt = jdatetime.datetime.fromgregorian(datetime=dt)
  return dt

# Function: Delete expired files
def del_files_s3():
  for o in bucket.objects.all():
    if time_localize(datetime.datetime.now()) - time_localize(o.last_modified) >= datetime.timedelta(hours=config["max_keep_hours"]):
      try:
        o.delete()
      except Exception as e:
        logger.error(f"Couldn't delete expired object '{o.key}': {e}")

# /start command
@app.on_message(filters.private & filters.incoming & filters.command("start"))
async def start(c: Client, m: Message):
  user = m.from_user
  logger.info("%s started the bot. User ID: %s, Username: %s" % (f"{user.first_name}{' ' + user.last_name if user.last_name != None else ''}", user.id, "@" + user.username if user.username != None else "None"))
  await m.reply(messages["start_msg"])
  del_files_s3()

# /help command
@app.on_message(filters.private & filters.incoming & filters.command("help"))
async def help(c: Client, m: Message):
  await m.reply(messages["help_msg"] % (humanize(config["max_file_size"][0] * 1024**2), humanize(config["max_file_size"][1] * 1024**2)), disable_web_page_preview=True)
  del_files_s3()

# /list command
@app.on_message(filters.private & filters.incoming & filters.command("list"))
async def list(c: Client, m: Message):
  user = m.from_user
  del_files_s3()
  if user.id in config["admins"]:
    count = sum(1 for _ in bucket.objects.all())
    if count > 0:
      list_str = ""
      i = 1
      for o in bucket.objects.all():
        list_str += f"{i}) <a href='{config['s3_dl_base_url']}/{urllib.parse.quote(o.key)}'>{o.key}</a>\n"
        i += 1
      msg_txt = f"{messages['list_title']}\n\n{list_str.strip()}"
      await m.reply(msg_txt, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True)
    else:
      await m.reply(messages["list_empty"], quote=True)
  else:
    await m.reply(messages["no_access"], quote=True)

# /delall command
@app.on_message(filters.private & filters.incoming & filters.command("delall"))
async def delall(c: Client, m: Message):
  user = m.from_user
  if user.id in config["admins"]:
    count = sum(1 for _ in bucket.objects.all())
    if count > 0:
      bucket.objects.all().delete()
      await m.reply(messages["delall_success"] % (count), quote=True)
      logger.info("%s deleted all files. Count: %s" % (user.id, count))
    else:
      await m.reply(messages["delall_already"], quote=True)
  else:
    await m.reply(messages["no_access"], quote=True)

# /delete (/del) command
@app.on_message(filters.private & filters.incoming & (filters.command("delete") | filters.command("del")))
async def delete(c: Client, m: Message):
  user = m.from_user
  if user.id in config["admins"]:
    cmd = m.text.split(" ")
    if len(cmd) > 1:
      obj_name = " ".join(cmd[1:])
      obj_list = bucket.objects.filter(Prefix=obj_name)
      if sum(1 for _ in obj_list) > 0:
        for o in obj_list:
          o.delete()
          logger.info("%s deleted an object: '%s'" % (user.id, obj_name))
        await m.reply(messages["del_obj_success"], quote=True)
      else:
        await m.reply(messages["del_obj_not_found"], quote=True)
    else:
      await m.reply(messages["del_cmd_error"], quote=True)
  else:
    await m.reply(messages["no_access"], quote=True)
  del_files_s3()

# When user sends a file
@app.on_message(filters.private & filters.incoming & (filters.document | filters.video | filters.photo | filters.animation | filters.audio | filters.voice | filters.video_note))
async def upload_file(c: Client, m: Message):
  user = m.from_user
  tempmsg = await m.reply(messages["file_check_tempmsg"], quote=True)
  if m.media:
    file = getattr(m, m.media.value)
    file_id = file.file_unique_id
  else:
    await m.reply(messages["file_not_support"])
    return
  url = ""
  for o in bucket.objects.all():
    if o.key.startswith(file_id):
      obj = o
      url = f"{config['s3_dl_base_url']}/{urllib.parse.quote(o.key)}"
      break
  file_size = int(file.file_size)
  if url and int(obj.size) == file_size:
    await tempmsg.delete()
    expire_date = format(time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"]), "%Y/%m/%d")
    expire_time = format(time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"]), "%H:%M:%S")
    await m.reply(messages["file_upload_already"] % (humanize(obj.size), url, expire_date, expire_time), quote=True, disable_web_page_preview=True)
  else:
    if user.id in (config["admins"] + config["vip_users"]):
      max_size = config["max_file_size"][1]
    else:
      max_size = config["max_file_size"][0]
    if (file_size / (1024**2)) <= max_size:
      try:
        file_path = await m.download(progress=dl_progress, progress_args=[tempmsg])
        await tempmsg.edit(messages["file_upload_tempmsg"])
        try:
          obj_name = f"{file_id}/{os.path.basename(file_path)}"
          # boto3 doesn't support async callback for file uploads
          bucket.upload_file(file_path, obj_name, ExtraArgs={"ACL": "public-read"})
          obj_url = f"{config['s3_dl_base_url']}/{urllib.parse.quote(obj_name)}"
          await tempmsg.delete()
          for o in bucket.objects.filter(Prefix=file_id):
            obj = o
            break
          expire_date = format(time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"]), "%Y/%m/%d")
          expire_time = format(time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"]), "%H:%M:%S")
          await m.reply(messages["file_upload_success"] % (humanize(obj.size), obj_url, expire_date, expire_time), quote=True, disable_web_page_preview=True)
          logger.info("%s uploaded an object: '%s'" % (user.id, obj_name))
          os.remove(file_path)
        except Exception as e:
          logger.error(f"Couldn't upload %s's object to S3 storage: {e}" % user.id)
          await tempmsg.delete()
          await m.reply(messages["file_upload_error"], quote=True)
          os.remove(file_path)
      except Exception as e:
        logger.error(f"Couldn't download %s's file: {e}" % user.id)
        await tempmsg.delete()
        await m.reply(messages["file_download_error"], quote=True)
    else:
      await tempmsg.delete()
      await m.reply(messages["file_size_error"] % humanize(max_size * 1024**2), quote=True)

# Calback Function: Track file download progress
async def dl_progress(current, total, tempmsg):
  prcnt = round(current * 100 / total, 1)
  prgrs_info = f"{humanize(current)} / {humanize(total)} ({prcnt}%)"
  full, empty = round(prcnt / 5), 20 - round(prcnt / 5)
  prgrs_bar = "".join([messages["progress_full_bar"] for i in range(full)] + [messages["progress_empty_bar"] for i in range(empty)])
  await tempmsg.edit(messages["file_download_tempmsg"] % (prgrs_info, prgrs_bar))
  await asyncio.sleep(4)

# When user sends anything but file
@app.on_message(filters.private & filters.incoming & ~filters.text & ~filters.document & ~filters.video & ~filters.photo & ~filters.animation & ~filters.audio & ~filters.voice & ~filters.video_note)
async def error_msg(c: Client, m: Message):
  await m.reply(messages["not_file_error"], quote=True)
  del_files_s3()

# Main function
def main():
  del_files_s3()
  app.run()
  
# On start
if __name__ == "__main__":
  main()