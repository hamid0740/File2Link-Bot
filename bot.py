# Modules & Libraries
import boto3
import yaml
import datetime
import jdatetime
import time
try:
  import zoneinfo
except ImportError:
  from backports import zoneinfo
import os
import logging
import urllib.parse
# Telegram
from pyrogram import Client, filters, enums, idle
from pyrogram.types import Message

# Set logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO, datefmt="%Y/%m/%d %H:%M:%S")
logger = logging.getLogger(__name__)

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
s3 = boto3.resource(
  "s3",
  endpoint_url=config["s3_endpoint_url"],
  aws_access_key_id=config["s3_access_key"],
  aws_secret_access_key=config["s3_secret_key"]
)
bucket = s3.Bucket(config["s3_bucket_name"])

# Function: Update environment variables
def update_env_vars():
  if len(os.environ.get("F2L_ADMINS", "").split(",")) > 0:
    config["admins"] += [int(x.strip()) for x in os.environ.get("F2L_ADMINS", "").split(",") if x.strip().isdigit() and int(x.strip()) not in config["admins"]]
  if len(os.environ.get("F2L_VIPS", "").split(",")) > 0:
    config["vip_users"] += [int(x.strip()) for x in os.environ.get("F2L_VIPS", "").split(",") if x.strip().isdigit() and int(x.strip()) not in config["vip_users"]]
  if len(os.environ.get("F2L_MAXSIZE", "").split(",")) == 2:
    list = os.environ.get("F2L_MAXSIZE", "").split(",")
    if list[0].strip().isdigit() and list[1].strip().isdigit():
      s1, s2 = int(list[0].strip()), int(list[1].strip())
      max_size = [s2, s1] if s1 > s2 else [s1, s2]
      config["max_file_size"] = max_size
  if os.environ.get("F2L_MAXHOUR", "").isdigit():
    config["max_keep_hours"] = int(os.environ.get("F2L_MAXHOUR", "")) 

# Function: Make file size human-readable
def humanize(num, suffix="B"):
  for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
    if abs(num) < 1024.0:
      return f"{num:3.1f}{unit}{suffix}"
    num /= 1024.0
  return f"{num:.1f}Yi{suffix}"

# Function: Localize time
def time_localize(dt, disable_jalali=False):
  dt = dt.astimezone(zoneinfo.ZoneInfo(config["timezone"]))
  if config["use_jalali_date"] and not disable_jalali:
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
async def start_cmd(c: Client, m: Message):
  user = m.from_user
  logger.info("%s started the bot. User ID: %s, Username: %s" % (f"{user.first_name}{' ' + user.last_name if user.last_name != None else ''}", user.id, "@" + user.username if user.username != None else "None"))
  await m.reply(messages["start_msg"])
  del_files_s3()

# /help command
@app.on_message(filters.private & filters.incoming & filters.command("help"))
async def help_cmd(c: Client, m: Message):
  update_env_vars()
  await m.reply(messages["help_msg"] % (humanize(config["max_file_size"][0] * 1024**2), humanize(config["max_file_size"][1] * 1024**2)), disable_web_page_preview=True)
  del_files_s3()

# /list command
@app.on_message(filters.private & filters.incoming & filters.command("list"))
async def list_cmd(c: Client, m: Message):
  user = m.from_user
  del_files_s3()
  if user.id in config["admins"]:
    count = sum(1 for _ in bucket.objects.all())
    if count > 0:
      list_str = ""
      i = 1
      for o in bucket.objects.all():
        expire_datetime = time_localize(o.last_modified) + datetime.timedelta(hours=config["max_keep_hours"])
        if config["use_presigned_url"]:
          obj_url = s3.meta.client.generate_presigned_url("get_object", Params={"Bucket": config["s3_bucket_name"], "Key": o.key}, ExpiresIn=(expire_datetime - time_localize(datetime.datetime.now())).total_seconds())
        else:
          obj_url = f"{config['s3_dl_base_url']}/{urllib.parse.quote(o.key)}"
        list_str += f"{i}) <a href='{obj_url}'>{o.key}</a>\n"
        i += 1
      msg_txt = f"{messages['list_title']}\n\n{list_str.strip()}"
      await m.reply(msg_txt, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True)
    else:
      await m.reply(messages["list_empty"], quote=True)
  else:
    await m.reply(messages["no_access"], quote=True)

# /delall command
@app.on_message(filters.private & filters.incoming & filters.command("delall"))
async def delall_cmd(c: Client, m: Message):
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
async def delete_cmd(c: Client, m: Message):
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
async def upload_file_cmd(c: Client, m: Message):
  user = m.from_user
  tempmsg = await m.reply(messages["file_check_tempmsg"], quote=True)
  if m.media:
    file = getattr(m, m.media.value)
    file_id = file.file_unique_id
  else:
    await m.reply(messages["file_not_support"])
    return
  obj = ""
  for o in bucket.objects.all():
    if o.key.startswith(file_id):
      obj = o
      break
  file_size = int(file.file_size)
  if obj and int(obj.size) == file_size:
    expire_datetime = time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"])
    if config["use_presigned_url"]:
      obj_url = s3.meta.client.generate_presigned_url("get_object", Params={"Bucket": config["s3_bucket_name"], "Key": o.key}, ExpiresIn=(expire_datetime - time_localize(datetime.datetime.now())).total_seconds())
    else:
      obj_url = f"{config['s3_dl_base_url']}/{urllib.parse.quote(o.key)}"
    expire_date = format(expire_datetime, "%Y/%m/%d")
    expire_time = format(expire_datetime, "%H:%M:%S")
    await tempmsg.delete()
    await m.reply(messages["file_upload_already"] % (humanize(obj.size), obj_url, expire_date, expire_time), quote=True, disable_web_page_preview=True)
  else:
    update_env_vars()
    if user.id in (config["admins"] + config["vip_users"]):
      max_size = config["max_file_size"][1]
    else:
      max_size = config["max_file_size"][0]
    if (file_size / (1024**2)) <= max_size:
      try:
        dl_start_time = time.time()
        file_path = await m.download(progress=dl_progress, progress_args=(tempmsg, dl_start_time))
        # TO-DO: add /cancel command to stop file download or upload
        await tempmsg.edit(messages["file_upload_tempmsg"])
        try:
          obj_name = f"{file_id}/{os.path.basename(file_path)}"
          # boto3 doesn't support async callback for file uploads
          bucket.upload_file(file_path, obj_name, ExtraArgs={"ACL": "public-read"})
          for o in bucket.objects.filter(Prefix=file_id):
            obj = o
            break
          expire_datetime = time_localize(obj.last_modified) + datetime.timedelta(hours=config["max_keep_hours"])
          if config["use_presigned_url"]:
            obj_url = s3.meta.client.generate_presigned_url("get_object", Params={"Bucket": config["s3_bucket_name"], "Key": o.key}, ExpiresIn=(expire_datetime - time_localize(datetime.datetime.now())).total_seconds())
          else:
            obj_url = f"{config['s3_dl_base_url']}/{urllib.parse.quote(o.key)}"
          expire_date = format(expire_datetime, "%Y/%m/%d")
          expire_time = format(expire_datetime, "%H:%M:%S")
          await tempmsg.delete()
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
async def dl_progress(current, total, tempmsg, start_time):
  if round((time.time() - start_time) % 5.00) == 0 or current == total:
    prcnt = round(current * 100 / total, 1)
    prgrs_info = f"{humanize(current)} / {humanize(total)} ({prcnt}%)"
    full, empty = round(prcnt / 5), 20 - round(prcnt / 5)
    prgrs_bar = "".join([messages["progress_full_bar"] for i in range(full)] + [messages["progress_empty_bar"] for i in range(empty)])
    await tempmsg.edit(messages["file_download_tempmsg"] % (prgrs_info, prgrs_bar))

# When user sends anything but file
@app.on_message(filters.private & filters.incoming & ~filters.text & ~filters.document & ~filters.video & ~filters.photo & ~filters.animation & ~filters.audio & ~filters.voice & ~filters.video_note)
async def not_file_cmd(c: Client, m: Message):
  await m.reply(messages["not_file_error"], quote=True)
  del_files_s3()

# Main function
def main():
  del_files_s3()
  update_env_vars()
  app.run()
  idle()
  app.stop()
  
# On start
if __name__ == "__main__":
  main()
