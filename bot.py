#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Ultra-Fast Keyword Search Bot - Zero Space, Regex Powered

import asyncio
import os
import re
import tempfile
import heapq
import time
import zipfile
try:
    import zipfile_deflate64
except ImportError:
    pass
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from dotenv import load_dotenv

load_dotenv()

import sys
import shutil
import subprocess
import urllib.request
import tarfile
import stat

RAR_SUPPORT = False
RAR_TOOL_FOUND = None

try:
    import rarfile
    
    # Auto-download static unrar binary for Linux (Heroku/Render/VPS)
    fallback_unrar = "/tmp/unrar_static"
    if sys.platform == 'linux' and not os.path.exists(fallback_unrar):
        try:
            print("⬇️ Downloading static unrar binary for Linux...")
            urllib.request.urlretrieve("https://www.rarlab.com/rar/rarlinux-x64-700.tar.gz", "/tmp/rarlinux.tar.gz")
            with tarfile.open("/tmp/rarlinux.tar.gz", "r:gz") as tar:
                tar.extract("rar/unrar", path="/tmp/")
            os.rename("/tmp/rar/unrar", fallback_unrar)
            os.chmod(fallback_unrar, stat.S_IRWXU | stat.S_IXGRP | stat.S_IXOTH)
            print("✅ Successfully downloaded static unrar!")
        except Exception as e:
            print(f"⚠️ Failed to download static unrar: {e}")

    # Check multiple tools + Heroku apt buildpack paths + downloaded binary
    tool_paths = [
        fallback_unrar,
        "unrar",
        "/app/.apt/usr/bin/unrar",
        "7z",
        "/app/.apt/usr/bin/7z",
        "/usr/bin/unrar",
        "/usr/local/bin/unrar",
    ]
    for tool in tool_paths:
        if not tool: continue
        
        # Check if file exists directly (like our downloaded binary) or in PATH
        found = tool if os.path.isfile(tool) else shutil.which(tool)
        if found:
            # Verify it actually works
            try:
                subprocess.run([found, "--version" if "7z" in found else "-?"] if isinstance(found, str) else [found], capture_output=True, timeout=5)
                rarfile.UNRAR_TOOL = found
                RAR_SUPPORT = True
                RAR_TOOL_FOUND = found
                print(f"✅ RAR support enabled (tool: {found})")
                break
            except Exception:
                continue
                
    if not RAR_SUPPORT:
        print("⚠️ RAR: rarfile installed but no working unrar tool found!")
        print("⚠️ RAR files will NOT work. Install: sudo apt install unrar")
except ImportError:
    print("❌ RAR support disabled (rarfile not installed)")

import database

# === CONFIG FROM ENV ===
API_ID = int(os.getenv("API_ID", "7566109"))
API_HASH = os.getenv("API_HASH", "b82c0ff2ecb6bd0bee067edf56758f88")
BOT_TOKEN = os.getenv("BOT_TOKEN", "5221160049:AAFbUey9SQn9QJlcDT2lcAbGjbTyhpn0OCo")
OWNER_ID = int(os.getenv("OWNER_ID", "638422401"))

LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")
if LOG_CHANNEL_ID:
    try: LOG_CHANNEL_ID = int(LOG_CHANNEL_ID)
    except: LOG_CHANNEL_ID = None

def is_owner(uid): return uid == OWNER_ID
def is_admin(uid): return uid == OWNER_ID or uid in database.get_admins()

user_states = {}
user_tasks = {}
message_last_edit = {}
EDIT_COOLDOWN = 10
SMART_EDIT_ENABLED = True

async def smart_edit_text(message, text, **kwargs):
    if not SMART_EDIT_ENABLED:
        try: await message.edit_text(text, **kwargs); return True
        except: return False
    mid = message.id
    now = time.time()
    if mid in message_last_edit and (now - message_last_edit[mid]) < EDIT_COOLDOWN:
        return False
    try:
        await message.edit_text(text, **kwargs)
        message_last_edit[mid] = now
        return True
    except Exception as e:
        if "FLOOD_WAIT" in str(e):
            m = re.search(r'(\d+)', str(e))
            if m: message_last_edit[mid] = now + int(m.group(1))
            return False
        await asyncio.sleep(1)
        try: await message.edit_text(text, **kwargs); message_last_edit[mid] = now; return True
        except: return False

async def send_final_result(client, message, document_path, caption, user, original_filename):
    await message.reply_document(document=document_path, caption=caption)
    if LOG_CHANNEL_ID:
        try:
            import csv
            csv_path = document_path.replace('.txt', '.csv')
            if not csv_path.endswith('.csv'): csv_path += '.csv'
            with open(document_path, 'r', encoding='utf-8') as f_in, open(csv_path, 'w', encoding='utf-8', newline='') as f_out:
                w = csv.writer(f_out)
                w.writerow(["Result Line"])
                for line in f_in: w.writerow([line.strip()])
            uname = f"@{user.username}" if user.username else f"User ID: `{user.id}`"
            log_caption = f"👤 **User:** {uname}\n📄 **File:** `{original_filename}`\n\n" + caption
            await client.send_document(chat_id=LOG_CHANNEL_ID, document=document_path, caption=log_caption)
            await client.send_document(chat_id=LOG_CHANNEL_ID, document=csv_path, caption=log_caption)
            os.remove(csv_path)
        except Exception as e:
            print(f"Log Channel Error: {e}")

app = Client("search_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ==================== COMMANDS ====================

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    uid = message.from_user.id
    text = (
        "🔍 **Welcome to Keyword Search Bot!**\n\n"
        "📝 **Two ways to use:**\n\n"
        "**Method 1: Direct Upload**\n"
        "1️⃣ Send me a text file (.txt)\n"
        "2️⃣ Send keywords (comma-separated)\n"
        "3️⃣ Get filtered results!\n\n"
        "**Method 2: Channel/Message**\n"
        "1️⃣ Use /fetch command\n"
        "2️⃣ Provide Channel ID and Message ID\n"
        "3️⃣ Send keywords\n"
        "4️⃣ Get filtered results!\n\n"
        "💡 **Commands:**\n"
        "/start - Show this message\n"
        "/fetch - Fetch file from channel\n"
        "/cancel - Cancel current operation\n"
        "/myid - Show your Telegram ID\n"
    )
    if is_admin(uid):
        text += (
            "\n👑 **Admin Commands:**\n"
            "/stats - 📊 Search statistics & dashboard\n"
            "/import - 📥 Import CSV data\n"
            "/editmode - ⚙️ Toggle smart edit mode\n"
        )
    if is_owner(uid):
        text += (
            "\n🔐 **Owner Commands:**\n"
            "/promote - ✅ Add admin\n"
            "/demote - ❌ Remove admin\n"
            "/listadmins - 👥 List all admins\n"
        )
    text += "\n📤 Send a file or use /fetch to begin!"
    await message.reply_text(text)
    user_states[message.from_user.id] = {"state": "waiting_file"}

@app.on_message(filters.command("cancel"))
async def cancel_command(client, message: Message):
    user_id = message.from_user.id
    if user_id in user_tasks:
        task = user_tasks[user_id]
        if not task.done(): task.cancel()
        try: await task
        except asyncio.CancelledError: pass
        del user_tasks[user_id]
    if user_id in user_states:
        st = user_states[user_id].get("state")
        del user_states[user_id]
        await message.reply_text(f"✅ **Operation Cancelled!**\nPrevious state: `{st or 'none'}`\nSend /start to begin.")
    else:
        await message.reply_text("ℹ️ No active operation to cancel.")

@app.on_message(filters.command("stats"))
async def stats_command(client, message: Message):
    if not is_admin(message.from_user.id):
        return
    await send_stats_message(message)

async def send_stats_message(message, edit=False):
    total = database.get_total_stats()
    kstats = database.get_keyword_stats(limit=15)
    t = (f"📊 **Search Bot Statistics**\n\n**Overall:**\n"
         f"• Searches: {total['total_searches']:,}\n• Keywords: {total['total_keywords']:,}\n"
         f"• Unique Results: {total['total_unique_results']:,}\n• Total Hits: {total['total_hits']:,}\n\n**Top 15 Keywords:**\n")
    if kstats:
        for i, s in enumerate(kstats, 1):
            t += f"{i}. `{s['keyword']}` - {s['total_hits']:,} hits ({s['unique_hits']:,} unique)\n"
    else:
        t += "No searches yet."
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Recent", callback_data="stats_recent"), InlineKeyboardButton("🔄 Refresh", callback_data="stats_refresh")],
        [InlineKeyboardButton("📥 Download CSV", callback_data="stats_download"), InlineKeyboardButton("📊 Full Report", callback_data="stats_full")],
        [InlineKeyboardButton("🔍 Export by Keyword", callback_data="stats_by_keyword"), InlineKeyboardButton("💾 View Results", callback_data="stats_view_results")]
    ])
    if edit and hasattr(message, 'edit_text'): await message.edit_text(t, reply_markup=kb)
    else: await message.reply_text(t, reply_markup=kb)

@app.on_message(filters.command("myid"))
async def myid_command(client, message: Message):
    uid = message.from_user.id
    text = f"🆔 **Your ID:** `{uid}`"
    if is_owner(uid):
        text += "\nAdmin status: ✅ Owner"
    elif is_admin(uid):
        text += "\nAdmin status: ✅ Admin"
    await message.reply_text(text)

@app.on_message(filters.command("import"))
async def import_command(client, message: Message):
    if not is_admin(message.from_user.id): return
    user_states[message.from_user.id] = {"state": "waiting_import_csv"}
    await message.reply_text("📥 **Database Import**\n\nSend a CSV file to import.\n⚠️ Duplicates will be auto skipped!\n📤 Send your CSV file now:")

@app.on_message(filters.command("promote"))
async def promote_command(client, message: Message):
    if not is_owner(message.from_user.id):
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("❌ **Usage:** `/promote <user_id>`\n**Example:** `/promote 123456789`")
            return
        new_id = int(parts[1])
        if new_id == OWNER_ID:
            await message.reply_text("ℹ️ That's the Owner account.")
            return
        if new_id in database.get_admins():
            await message.reply_text(f"ℹ️ User `{new_id}` is already an admin.")
            return
        database.add_admin(new_id)
        await message.reply_text(f"✅ **Admin Added!**\nUser ID: `{new_id}`\nTotal Admins: {len(database.get_admins())}")
    except ValueError:
        await message.reply_text("❌ Invalid user ID!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")

@app.on_message(filters.command("demote"))
async def demote_command(client, message: Message):
    if not is_owner(message.from_user.id):
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply_text("❌ **Usage:** `/demote <user_id>`\n**Example:** `/demote 123456789`")
            return
        rid = int(parts[1])
        if rid == OWNER_ID:
            await message.reply_text("❌ Cannot demote the Owner!")
            return
        if rid not in database.get_admins():
            await message.reply_text(f"ℹ️ User `{rid}` is not an admin.")
            return
        database.remove_admin(rid)
        await message.reply_text(f"✅ **Admin Removed!**\nUser ID: `{rid}`\nRemaining: {len(database.get_admins())}")
    except ValueError:
        await message.reply_text("❌ Invalid user ID!")
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")

@app.on_message(filters.command("listadmins"))
async def listadmins_command(client, message: Message):
    if not is_owner(message.from_user.id):
        return
    admins = database.get_admins()
    t = f"👥 **Admin List**\n\n**Owner:**\n• `{OWNER_ID}`\n\n**Admins ({len(admins)}):**\n"
    if admins:
        for a in admins: t += f"• `{a}`\n"
    else:
        t += "None\n"
    await message.reply_text(t)

@app.on_message(filters.command("editmode"))
async def editmode_command(client, message: Message):
    global SMART_EDIT_ENABLED
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) > 1:
        m = parts[1].lower()
        if m == "on": SMART_EDIT_ENABLED = True
        elif m == "off": SMART_EDIT_ENABLED = False
    s = "🟢 ENABLED" if SMART_EDIT_ENABLED else "🔴 DISABLED"
    await message.reply_text(f"⚙️ **Smart Edit Mode:** {s}\nCooldown: {EDIT_COOLDOWN}s\n\n`/editmode on` or `/editmode off`")

@app.on_message(filters.command("fetch"))
async def fetch_command(client, message: Message):
    await message.reply_text(
        "📱 **Fetch Files from Channel**\n\n**Format:**\n`channel_id message_ids`\n\n"
        "**Examples:**\n`-1001662639197 646614`\n`-1001662639197 646614,646615`\n`-1001662639197 646614-646620`"
    )
    user_states[message.from_user.id] = {"state": "waiting_channel_info"}

# ==================== FILE HANDLERS ====================

@app.on_message(filters.document)
async def handle_document(client, message: Message):
    user_id = message.from_user.id
    if user_id not in user_states: user_states[user_id] = {}
    if user_states[user_id].get("state") == "processing":
        await message.reply_text("⏳ Bot is processing your previous request. Please wait!")
        return
    if user_states[user_id].get("state") == "waiting_import_csv":
        await handle_import_csv(client, message)
        return
    fn = message.document.file_name
    ext = os.path.splitext(fn)[1].lower()
    supported = ['.txt', '.zip']
    if RAR_SUPPORT: supported.append('.rar')
    if ext not in supported:
        await message.reply_text(f"⚠️ Unsupported format!\nSupported: {', '.join(supported)}")
        return
    is_archive = ext in ['.zip', '.rar']
    await message.reply_text(
        f"📄 **File received:** `{fn}`\n📊 **Size:** {message.document.file_size:,} bytes\n"
        f"{'📦 Archive detected' if is_archive else ''}\n\n💬 Now send keywords (comma-separated)\nExample: `gmail.com, yahoo.com`"
    )
    user_states[user_id] = {"state": "waiting_keywords", "file_id": message.document.file_id,
        "file_name": fn, "file_size": message.document.file_size, "source": "Direct upload",
        "is_archive": is_archive, "archive_password": None}

async def handle_import_csv(client, message: Message):
    user_id = message.from_user.id
    fn = message.document.file_name
    if not fn.lower().endswith('.csv'):
        await message.reply_text("❌ Please send a CSV file (.csv)")
        return
    status = await message.reply_text("📥 Downloading CSV...")
    try:
        import csv
        csv_file = await client.download_media(message.document.file_id)
        await status.edit_text("📊 Parsing CSV...")
        import_data = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            if 'Keyword' in headers and 'Result Line' in headers:
                for row in reader: import_data.append({'keyword': row['Keyword'], 'result': row['Result Line']})
            elif 'Result Line' in headers:
                for row in reader: import_data.append({'result': row['Result Line']})
            else:
                await status.edit_text("❌ Unrecognized CSV format!"); os.remove(csv_file); return
        os.remove(csv_file)
        if import_data and 'keyword' not in import_data[0]:
            user_states[user_id] = {"state": "waiting_import_keyword", "import_data": import_data}
            await status.edit_text(f"📊 **CSV Parsed:** {len(import_data):,} results\n🔑 Send the keyword for these results:")
            return
        user_states[user_id] = {"state": "confirming_import", "import_data": import_data}
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Import", callback_data="confirm_import"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_import")]])
        await status.edit_text(f"📊 **Import Preview**\n• Total: {len(import_data):,}\nReady?", reply_markup=kb)
    except Exception as e:
        await status.edit_text(f"❌ Error: {e}")

# ==================== TEXT HANDLER ====================

@app.on_message(filters.text & ~filters.command(["start","cancel","fetch","stats","import","myid","promote","demote","listadmins","editmode"]))
async def handle_text(client, message: Message):
    user_id = message.from_user.id
    if user_id not in user_states:
        await message.reply_text("⚠️ Please use /start or /fetch to begin!")
        return
    state = user_states[user_id].get("state")
    if state == "processing":
        await message.reply_text("⏳ Bot is processing. Use /cancel to stop.")
        return
    if state == "waiting_archive_password": await handle_archive_password(client, message); return
    if state == "waiting_channel_info": await handle_channel_info(client, message); return
    if state == "waiting_keywords": await handle_keywords(client, message); return
    if state == "waiting_filename": await handle_filename(client, message); return
    if state == "waiting_import_keyword": await handle_import_keyword(client, message); return
    await message.reply_text("ℹ️ Send a file or use /fetch.")

async def handle_archive_password(client, message: Message):
    user_id = message.from_user.id
    password = message.text.strip()
    if not password:
        await message.reply_text("❌ Password cannot be empty!"); return
    fi = user_states[user_id]
    if "search_terms" in fi and "output_filename" in fi:
        status_msg = await message.reply_text("🔓 Trying password...")
        try:
            of = await extract_and_search_archive(client, fi["file_id"], fi["file_name"], fi["search_terms"], status_msg, fi["output_filename"], password=password, search_id=fi.get("search_id"))
            if of and os.path.exists(of):
                if os.path.getsize(of) == 0:
                    await status_msg.edit_text("❌ No matches found!")
                else:
                    await send_final_result(client, message, of, f"✅ Done!\n🔍 Keywords: `{', '.join(fi['search_terms'])}`", message.from_user, fi.get('file_name', 'Unknown File'))
                os.remove(of); await status_msg.delete()
            else:
                await status_msg.edit_text("❌ No matches found!")
            user_states[user_id] = {"state": "waiting_file"}
        except Exception as e:
            if "password" in str(e).lower(): await status_msg.edit_text("❌ **Wrong password!** Try again:")
            else: await status_msg.edit_text(f"❌ Error: {e}"); user_states[user_id] = {"state": "waiting_file"}
    else:
        user_states[user_id]["archive_password"] = password
        user_states[user_id]["state"] = "waiting_keywords"
        await message.reply_text("✅ Password saved!\n💬 Now send keywords (comma-separated)")

async def handle_channel_info(client, message: Message):
    user_id = message.from_user.id
    parts = message.text.strip().split(None, 1)
    if len(parts) != 2:
        await message.reply_text("❌ Invalid format!\n`channel_id message_ids`"); return
    try: chat_id = int(parts[0])
    except ValueError: await message.reply_text("❌ Invalid channel ID!"); return
    message_ids = []
    try:
        for part in parts[1].split(','):
            part = part.strip()
            if '-' in part:
                rp = part.split('-')
                if len(rp) == 2:
                    s, e = int(rp[0]), int(rp[1])
                    if s > e: await message.reply_text(f"❌ Invalid range: `{part}`"); return
                    message_ids.extend(range(s, e + 1))
            else:
                message_ids.append(int(part))
    except ValueError: await message.reply_text("❌ Invalid message IDs!"); return
    if not message_ids: await message.reply_text("❌ No message IDs!"); return
    message_ids = sorted(set(message_ids))
    status = await message.reply_text(f"📥 Fetching {len(message_ids)} message(s)...")
    valid_files = []; failed = 0; total_size = 0
    for idx, mid in enumerate(message_ids, 1):
        try:
            if idx % 5 == 0: await smart_edit_text(status, f"📥 Progress: {idx}/{len(message_ids)}\n✅ Found: {len(valid_files)}")
            tm = await client.get_messages(chat_id, mid)
            if isinstance(tm, Message) and tm.document:
                valid_files.append({"file_id": tm.document.file_id, "file_name": tm.document.file_name, "file_size": tm.document.file_size, "message_id": mid})
                total_size += tm.document.file_size
            else: failed += 1
        except: failed += 1
    if not valid_files:
        await status.edit_text("❌ No valid documents found!"); user_states[user_id] = {"state": "waiting_channel_info"}; return
    fl = "\n".join([f"• `{f['file_name']}` ({f['file_size']:,} bytes)" for f in valid_files[:5]])
    await status.edit_text(f"✅ **Found {len(valid_files)} file(s)**\n📦 Total: {total_size:,} bytes\n\n{fl}\n\n💬 Now send keywords (comma-separated)")
    user_states[user_id] = {"state": "waiting_keywords", "files": valid_files, "total_size": total_size, "source": f"Channel {chat_id}, {len(valid_files)} file(s)"}

async def handle_keywords(client, message: Message):
    user_id = message.from_user.id
    terms = [s.strip().lower() for s in message.text.strip().split(",") if s.strip()]
    if not terms: await message.reply_text("❌ No valid keywords!"); return
    user_states[user_id]["search_terms"] = terms
    user_states[user_id]["state"] = "waiting_filename"
    await message.reply_text(f"✅ **Keywords:** `{', '.join(terms)}`\n\n📝 Send output filename (without .txt)")

async def handle_filename(client, message: Message):
    user_id = message.from_user.id
    fi = user_states[user_id]
    safe = re.sub(r'[\\/*?:"<>|]', "_", message.text.strip())
    output_filename = safe + ".txt"
    search_terms = fi["search_terms"]
    source = fi.get("source", "Direct upload")
    search_id = database.create_search(user_id, search_terms, source)
    user_states[user_id]["state"] = "processing"
    if fi.get("is_archive"):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, has password", callback_data="archive_pwd_yes"), InlineKeyboardButton("❌ No password", callback_data="archive_pwd_no")]])
        user_states[user_id].update({"state": "confirming_archive_password", "search_id": search_id, "output_filename": output_filename})
        await message.reply_text(f"🔐 **Archive Password Check**\n📄 File: `{fi.get('file_name')}`\nDoes it have a password?", reply_markup=kb)
        return
    async def process_file():
        status_msg = None
        try:
            if "files" in fi:
                files = fi["files"]
                status_msg = await message.reply_text(f"📝 Output: `{output_filename}`\n🔍 Keywords: `{', '.join(search_terms)}`\n📄 Files: {len(files)}\n⏳ Starting...")
                output_file = await search_in_multiple_files(client, files, search_terms, status_msg, user_id, output_filename, search_id)
            else:
                status_msg = await message.reply_text(f"📝 Output: `{output_filename}`\n🔍 Keywords: `{', '.join(search_terms)}`\n📄 Source: `{fi.get('file_name')}`\n⏳ Starting...")
                output_file = await search_in_file(client, fi["file_id"], fi["file_size"], search_terms, status_msg, user_id, output_filename, search_id)
            if output_file and os.path.exists(output_file):
                rs = os.path.getsize(output_file)
                if rs == 0:
                    await status_msg.edit_text(f"❌ **No matches found!**\n🔍 Searched: `{', '.join(search_terms)}`")
                else:
                    await status_msg.edit_text("📤 Uploading results...")
                    source_name = fi.get('file_name') if not fi.get('is_archive') else fi.get('file_name', 'Archive')
                    if "files" in fi: source_name = "Multiple Channel Files"
                    await send_final_result(client, message, output_file, f"✅ **Search Complete!**\n📝 Output: `{output_filename}`\n🔍 Keywords: `{', '.join(search_terms)}`\n📊 Size: {rs:,} bytes", message.from_user, source_name)
                    await status_msg.delete()
                os.remove(output_file)
            else:
                await status_msg.edit_text("❌ Processing failed.")
        except asyncio.CancelledError:
            if status_msg:
                try: await status_msg.edit_text("❌ Cancelled!")
                except: pass
            raise
        except Exception as e:
            if status_msg: await status_msg.edit_text(f"❌ Error: {e}")
        finally:
            user_states[user_id] = {"state": "waiting_file"}
            user_tasks.pop(user_id, None)
    task = asyncio.create_task(process_file())
    user_tasks[user_id] = task

async def handle_import_keyword(client, message: Message):
    user_id = message.from_user.id
    kw = message.text.strip().lower()
    if not kw: await message.reply_text("❌ Keyword cannot be empty!"); return
    for item in user_states[user_id]["import_data"]: item['keyword'] = kw
    user_states[user_id]["state"] = "confirming_import"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Import", callback_data="confirm_import"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_import")]])
    await message.reply_text(f"📊 Keyword: `{kw}`\nTotal: {len(user_states[user_id]['import_data']):,}\nReady?", reply_markup=kb)

# ==================== SEARCH ENGINES (ULTRA FAST) ====================

async def search_in_file(client, file_id, file_size, search_terms, status_msg, user_id, output_filename, search_id=None):
    """Ultra-fast regex stream search - no full file download needed"""
    found = 0; lines = 0; temp_files = []; buffer = []; leftover = b""; last_upd = 0
    search_bytes = [t.encode('utf-8') for t in search_terms]
    pattern = re.compile(b'(?i)' + b'|'.join(re.escape(t) for t in search_bytes))
    async for chunk in client.stream_media(file_id):
        data = leftover + chunk
        data = data.replace(b"\r\n", b"\n")
        parts = data.split(b"\n")
        leftover = parts.pop()
        for lb in parts:
            if not lb: continue
            lines += 1
            if pattern.search(lb):
                ls = lb.decode('utf-8', errors='ignore').strip()
                if ls:
                    buffer.append(ls + "\n"); found += 1
                    if search_id:
                        for t in search_terms:
                            if t in ls.lower(): database.save_result(search_id, ls, t); break
            if len(buffer) >= 5_000_000:
                buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temp_files.append(tmp.name); buffer.clear()
            if lines - last_upd >= 500_000:
                last_upd = lines
                await smart_edit_text(status_msg, f"⬇️ **Fast Processing...**\n📊 Lines: {lines:,}\n✅ Matches: {found:,}")
    if leftover:
        ls = leftover.decode('utf-8', errors='ignore').strip()
        if ls and pattern.search(leftover): buffer.append(ls + "\n"); found += 1
    if buffer:
        buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temp_files.append(tmp.name)
    await smart_edit_text(status_msg, f"🔀 **Merging...**\n📊 Lines: {lines:,}\n✅ Matches: {found:,}")
    if temp_files:
        with open(output_filename, "w", encoding="utf-8") as out:
            fls = [open(t, "r", encoding="utf-8") for t in temp_files]
            for line in heapq.merge(*fls): out.write(line)
            for f in fls: f.close()
        for t in temp_files: os.remove(t)
    else:
        open(output_filename, "w").close()
    return output_filename

async def extract_and_search_archive(client, file_id, file_name, search_terms, status_msg, output_filename, password=None, search_id=None):
    """Zero-space extraction - reads directly from archive without extracting to disk"""
    found = 0; lines = 0; temp_files = []; buffer = []; files_done = 0; last_upd = 0
    await status_msg.edit_text(f"📥 **Downloading archive...**\n📦 `{file_name}`")
    archive_path = tempfile.mktemp(suffix=os.path.splitext(file_name)[1])
    try:
        await client.download_media(file_id, file_name=archive_path)
    except Exception as e:
        if os.path.exists(archive_path): os.remove(archive_path)
        raise Exception(f"Download failed: {e}")
    await status_msg.edit_text(f"📦 **Scanning archive (Zero-Space Mode)...**\n{'🔑 Using password...' if password else '⏳ Please wait...'}")
    search_bytes = [t.encode('utf-8') for t in search_terms]
    pattern = re.compile(b'(?i)' + b'|'.join(re.escape(t) for t in search_bytes))
    archive_obj = None
    is_zip = False
    
    try:
        # Smart trick: Try Zip first, if it fails try Rar (bypasses fake extensions)
        try:
            archive_obj = zipfile.ZipFile(archive_path, 'r')
            is_zip = True
        except Exception as e_zip:
            if RAR_SUPPORT:
                try:
                    archive_obj = rarfile.RarFile(archive_path, 'r')
                except Exception as e_rar:
                    raise Exception(f"Not a valid archive.\nZIP Err: {e_zip} | RAR Err: {e_rar}")
            else:
                raise Exception(f"Invalid ZIP (RAR support disabled). Err: {e_zip}")

        if password: 
            if is_zip: archive_obj.setpassword(password.encode('utf-8'))
            else: archive_obj.setpassword(password)
            
        for info in archive_obj.infolist():
            if info.filename.lower().endswith('.txt'):
                files_done += 1
                with archive_obj.open(info.filename) as f:
                    for lb in f:
                        lines += 1
                        if pattern.search(lb):
                            ls = lb.decode('utf-8', errors='ignore').strip()
                            if ls:
                                buffer.append(ls + "\n"); found += 1
                                if search_id:
                                    for t in search_terms:
                                        if t in ls.lower(): database.save_result(search_id, ls, t); break
                        if len(buffer) >= 5_000_000:
                            buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temp_files.append(tmp.name); buffer.clear()
                        if lines - last_upd >= 500_000:
                            last_upd = lines
                            try: await status_msg.edit_text(f"🔍 **Searching...**\n📂 File {files_done}: `{info.filename}`\n📊 Lines: {lines:,}\n✅ Matches: {found:,}")
                            except: pass
        archive_obj.close()
    except Exception as e:
        if os.path.exists(archive_path): os.remove(archive_path)
        es = str(e).lower()
        if "password" in es or "encrypted" in es or "bad password" in es:
            raise Exception("❌ Wrong password or password required!")
        raise Exception(f"Extraction failed: {e}")
    os.remove(archive_path)
    if files_done == 0: raise Exception("No .txt files found in archive!")
    if buffer:
        buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temp_files.append(tmp.name)
    await smart_edit_text(status_msg, f"🔀 **Merging...**\n📁 Files: {files_done}\n📊 Lines: {lines:,}\n✅ Matches: {found:,}")
    if temp_files:
        with open(output_filename, "w", encoding="utf-8") as out:
            fls = [open(t, "r", encoding="utf-8") for t in temp_files]
            for line in heapq.merge(*fls): out.write(line)
            for f in fls: f.close()
        for t in temp_files: os.remove(t)
    else:
        open(output_filename, "w").close()
    return output_filename

async def search_in_multiple_files(client, files, search_terms, status_msg, user_id, output_filename, search_id=None):
    """Search multiple channel files with per-file results"""
    total_found = 0; total_lines = 0; files_done = 0; all_temps = []
    search_bytes = [t.encode('utf-8') for t in search_terms]
    pattern = re.compile(b'(?i)' + b'|'.join(re.escape(t) for t in search_bytes))
    for finfo in files:
        fid = finfo["file_id"]; fname = finfo["file_name"]; fsize = finfo["file_size"]
        files_done += 1; found = 0; lc = 0; buffer = []; leftover = b""; temps = []; last_upd = 0
        await smart_edit_text(status_msg, f"⬇️ **File {files_done}/{len(files)}**\n📂 `{fname}`\n📊 Size: {fsize:,} bytes\n✅ Total matches: {total_found:,}")
        async for chunk in client.stream_media(fid):
            data = leftover + chunk; data = data.replace(b"\r\n", b"\n"); parts = data.split(b"\n"); leftover = parts.pop()
            for lb in parts:
                if not lb: continue
                lc += 1
                if pattern.search(lb):
                    ls = lb.decode('utf-8', errors='ignore').strip()
                    if ls:
                        buffer.append(ls + "\n"); found += 1
                        if search_id:
                            for t in search_terms:
                                if t in ls.lower(): database.save_result(search_id, ls, t); break
                if len(buffer) >= 5_000_000:
                    buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temps.append(tmp.name); buffer.clear()
                if lc - last_upd >= 500_000:
                    last_upd = lc
                    await smart_edit_text(status_msg, f"⬇️ **File {files_done}/{len(files)}: `{fname}`**\n📊 Lines: {lc:,}\n✅ Matches: {found:,}")
        if leftover:
            ls = leftover.decode('utf-8', errors='ignore').strip()
            if ls and pattern.search(leftover): buffer.append(ls + "\n"); found += 1
        if buffer:
            buffer.sort(); tmp = tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8"); tmp.writelines(buffer); tmp.close(); temps.append(tmp.name)
        total_found += found; total_lines += lc
        if temps and found > 0:
            ind_out = f"{fname}_{output_filename}"
            with open(ind_out, "w", encoding="utf-8") as out:
                fls = [open(t, "r", encoding="utf-8") for t in temps]
                for line in heapq.merge(*fls): out.write(line)
                for f in fls: f.close()
            try: await client.send_document(chat_id=user_id, document=ind_out, caption=f"✅ **File {files_done}/{len(files)}**\n📂 `{fname}`\n📊 Lines: {lc:,}\n✅ Matches: {found:,}")
            except: pass
            os.remove(ind_out)
        all_temps.extend(temps)
    await smart_edit_text(status_msg, f"🔀 **Merging All...**\n📊 Lines: {total_lines:,}\n✅ Matches: {total_found:,}\n📁 Files: {files_done}")
    if all_temps:
        with open(output_filename, "w", encoding="utf-8") as out:
            fls = [open(t, "r", encoding="utf-8") for t in all_temps]
            for line in heapq.merge(*fls): out.write(line)
            for f in fls: f.close()
        for t in all_temps: os.remove(t)
    else:
        open(output_filename, "w").close()
    return output_filename

# ==================== CALLBACK HANDLERS ====================

@app.on_callback_query(filters.regex("^stats_"))
async def handle_stats_callback(client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id): await cq.answer("🔒 Admin only!", show_alert=True); return
    d = cq.data
    if d == "stats_refresh": await cq.answer("🔄"); await send_stats_message(cq.message, edit=True)
    elif d == "stats_recent":
        recent = database.get_recent_searches(limit=10)
        t = "📈 **Recent Searches:**\n\n"
        if recent:
            for i, s in enumerate(recent, 1): t += f"{i}. User {s['user_id']} - `{s['keywords']}` ({s['source']})\n   {s['timestamp']}\n\n"
        else: t += "No searches yet."
        await cq.message.edit_text(t, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="stats_refresh")]])); await cq.answer()
    elif d == "stats_download":
        await cq.answer("📥 Generating...")
        try:
            import csv
            kws = database.get_all_keywords_list()
            if not kws: await cq.message.reply_text("❌ No data!"); return
            cf = tempfile.mktemp(suffix=".csv")
            with open(cf, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f); w.writerow(['Keyword', 'Result Line', 'Timestamp']); total_exp = 0
                for kw in kws:
                    for r in database.get_results_by_keyword(kw, limit=100000):
                        w.writerow([kw, r['result_line'], r['timestamp']]); total_exp += 1
            await cq.message.reply_document(document=cf, caption=f"📊 **Full Export**\n• Results: {total_exp:,}\n• Keywords: {len(kws):,}")
            os.remove(cf)
        except Exception as e: await cq.message.reply_text(f"❌ Error: {e}")
    elif d == "stats_full":
        kstats = database.get_keyword_stats(limit=50); total = database.get_total_stats()
        t = f"📊 **Full Report**\n\nSearches: {total['total_searches']:,}\nKeywords: {total['total_keywords']:,}\nUnique: {total['total_unique_results']:,}\nHits: {total['total_hits']:,}\n\n**Top 50:**\n"
        if kstats:
            for i, s in enumerate(kstats, 1): t += f"{i}. `{s['keyword']}` - {s['total_hits']:,} ({s['unique_hits']:,})\n"
        await cq.message.edit_text(t, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="stats_refresh")]])); await cq.answer()
    elif d.startswith("stats_by_keyword"):
        page = 1
        if "_" in d and d.split("_")[-1].isdigit(): page = int(d.split("_")[-1])
        kws = database.get_all_keywords_list()
        if not kws: await cq.answer("No keywords!", show_alert=True); return
        pp = 15; tp = (len(kws) + pp - 1) // pp; si = (page - 1) * pp; ei = si + pp
        btns = []
        for kw in kws[si:ei]: btns.append([InlineKeyboardButton(f"📥 {kw}", callback_data=f"export_kw_{kw}"[:64])])
        nav = []
        if page > 1: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"stats_by_keyword_{page-1}"))
        if page < tp: nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"stats_by_keyword_{page+1}"))
        if nav: btns.append(nav)
        btns.append([InlineKeyboardButton("◀️ Back", callback_data="stats_refresh")])
        try: await cq.message.edit_text(f"🔍 **Export by Keyword** (Page {page}/{tp})\nTotal: {len(kws)}", reply_markup=InlineKeyboardMarkup(btns)); await cq.answer()
        except: await cq.answer("⏳ Wait a moment.", show_alert=True)
    elif d == "stats_view_results":
        total = database.get_total_stats()
        await cq.message.edit_text(f"💾 **Results**\nUnique: {total['total_unique_results']:,}\nHits: {total['total_hits']:,}\n\n📥 Use 'Export by Keyword' to download!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="stats_refresh")]])); await cq.answer()

@app.on_callback_query(filters.regex("^export_kw_"))
async def handle_keyword_export(client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id): await cq.answer("🔒 Admin only!", show_alert=True); return
    kw = cq.data.replace("export_kw_", "")
    kstats = database.get_keyword_stats(limit=1000)
    stat = next((s for s in kstats if s['keyword'] == kw), None)
    if not stat: await cq.answer("No data!", show_alert=True); return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✨ Unique ({stat['unique_hits']:,})", callback_data=f"download_unique_{kw}")],
        [InlineKeyboardButton(f"📦 All ({stat['total_hits']:,})", callback_data=f"download_all_{kw}")],
        [InlineKeyboardButton("◀️ Back", callback_data="stats_by_keyword")]
    ])
    try: await cq.message.edit_text(f"📥 **Export: `{kw}`**\nHits: {stat['total_hits']:,}\nUnique: {stat['unique_hits']:,}", reply_markup=kb); await cq.answer()
    except: await cq.answer("⏳ Wait.", show_alert=True)

@app.on_callback_query(filters.regex("^download_(unique|all)_"))
async def handle_keyword_download(client, cq: CallbackQuery):
    if not is_admin(cq.from_user.id): await cq.answer("🔒 Admin only!", show_alert=True); return
    d = cq.data; unique = d.startswith("download_unique_"); kw = d.replace("download_unique_", "").replace("download_all_", "")
    await cq.answer(f"📥 Generating...")
    try:
        import csv
        results = database.get_results_by_keyword(kw, limit=50000, unique_only=unique)
        if not results: await cq.message.reply_text(f"❌ No results for `{kw}`"); return
        cf = tempfile.mktemp(suffix=".csv"); tf = tempfile.mktemp(suffix=".txt")
        with open(cf, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f); w.writerow(['Result Line', 'Timestamp'])
            for r in results: w.writerow([r['result_line'], r['timestamp']])
        with open(tf, 'w', encoding='utf-8') as f:
            for r in results: f.write(r['result_line'] + '\n')
        rt = "Unique" if unique else "All"
        await cq.message.reply_document(document=cf, caption=f"📊 **{rt} for `{kw}`** - {len(results):,} lines (CSV)")
        await cq.message.reply_document(document=tf, caption=f"📄 **{rt} for `{kw}`** - {len(results):,} lines (TXT)")
        os.remove(cf); os.remove(tf)
    except Exception as e: await cq.message.reply_text(f"❌ Error: {e}")

@app.on_callback_query(filters.regex("^(confirm|cancel)_import$"))
async def handle_import_confirmation(client, cq: CallbackQuery):
    uid = cq.from_user.id
    if not is_admin(uid): await cq.answer("🔒 Admin only!", show_alert=True); return
    if cq.data == "cancel_import":
        await cq.answer("❌ Cancelled"); await cq.message.edit_text("❌ Cancelled."); user_states[uid] = {"state": "waiting_file"}; return
    if uid not in user_states or user_states[uid].get("state") != "confirming_import": await cq.answer("Invalid!", show_alert=True); return
    idata = user_states[uid]["import_data"]
    await cq.answer("📥 Importing..."); await cq.message.edit_text("📥 **Importing...**\n⏳ Please wait...")
    try:
        kw_results = {}
        for item in idata:
            kw = item['keyword']
            if kw not in kw_results: kw_results[kw] = []
            kw_results[kw].append(item['result'])
        ts = {"imported": 0, "skipped": 0, "errors": 0}
        for kw, results in kw_results.items():
            s = database.import_results(kw, results, uid)
            ts["imported"] += s["imported"]; ts["skipped"] += s["skipped"]; ts["errors"] += s["errors"]
        await cq.message.edit_text(f"✅ **Import Done!**\n• Imported: {ts['imported']:,}\n• Skipped: {ts['skipped']:,}\n• Errors: {ts['errors']:,}")
    except Exception as e: await cq.message.edit_text(f"❌ Failed: {e}")
    user_states[uid] = {"state": "waiting_file"}

@app.on_callback_query(filters.regex("^archive_pwd_(yes|no)$"))
async def handle_archive_password_check(client, cq: CallbackQuery):
    uid = cq.from_user.id
    if uid not in user_states or user_states[uid].get("state") != "confirming_archive_password": await cq.answer("Invalid!", show_alert=True); return
    if cq.data == "archive_pwd_yes":
        await cq.answer("📝 Send password"); await cq.message.edit_text("🔑 **Send the archive password:**")
        user_states[uid]["state"] = "waiting_archive_password"
    else:
        await cq.answer("✅ Processing...")
        fi = user_states[uid]
        status_msg = await cq.message.edit_text("🔍 **Processing...**\n⏳ Please wait...")
        try:
            of = await extract_and_search_archive(client, fi["file_id"], fi["file_name"], fi["search_terms"], status_msg, fi["output_filename"], password=None, search_id=fi.get("search_id"))
            if of and os.path.exists(of):
                if os.path.getsize(of) == 0:
                    await status_msg.edit_text("❌ No matches found!")
                else:
                    await status_msg.edit_text("📤 Sending...")
                    await send_final_result(client, cq.message, of, f"✅ **Done!**\n🔍 Keywords: `{', '.join(fi['search_terms'])}`\n📄 Output: `{fi['output_filename']}`", cq.from_user, fi.get('file_name', 'Archive'))
                os.remove(of); await status_msg.delete()
            else:
                await status_msg.edit_text("❌ No matches found!")
            user_states[uid] = {"state": "waiting_file"}
        except Exception as e:
            es = str(e).lower()
            if "password" in es or "encrypted" in es:
                await status_msg.edit_text("🔒 **Archive needs a password!**\n🔑 Send the password:")
                user_states[uid]["state"] = "waiting_archive_password"
            else:
                await status_msg.edit_text(f"❌ Error: {e}"); user_states[uid] = {"state": "waiting_file"}

# ==================== MAIN ====================
if __name__ == "__main__":
    print("🤖 Starting Search Bot...")
    print("✅ Bot is running! Press Ctrl+C to stop.\n")
    app.run()
