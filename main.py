"""
Quranic Speech Dataset Collector — Backend v4
FastAPI + python-telegram-bot (standard Bot API) + Chunked 500MB batch upload

No local telegram-bot-api binary needed.
Strategy: split the 500MB ZIP into ≤45MB chunks and send each as a document.
The first chunk's message_id is stored so chunks can be sent as replies,
keeping them grouped in the Telegram channel.
"""

import os
import re
import csv
import json
import uuid
import asyncio
import zipfile
import shutil
import logging
import math
from pathlib import Path
from datetime import datetime

import aiofiles
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from supabase import create_client, Client

from telegram import Bot
from telegram.request import HTTPXRequest

import random

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("quran-collector")

# ─────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────
# Required secrets (HF Spaces → Settings → Repository secrets):
#
#   TELEGRAM_BOT_TOKEN   — from @BotFather
#   TELEGRAM_CHANNEL     — channel numeric ID e.g. "-1001234567890"
#   SUPABASE_URL         — from Supabase project → Settings → API
#   SUPABASE_KEY         — anon/public key from same page

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHANNEL   = os.environ["TELEGRAM_CHANNEL"]
SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_KEY       = os.environ["SUPABASE_KEY"]

BATCH_SIZE_BYTES = 500 * 1024 * 1024        # 500 MB — trigger threshold
CHUNK_SIZE_BYTES = 45 * 1024 * 1024         # 45 MB  — safe under 50 MB Bot API limit
TEMP_DIR         = Path("/tmp/quran_batch")
QURAN_JSON_PATH  = Path("quran_full.json")

# ─────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────
app = FastAPI(title="Quranic Speech Collector", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# Supabase Client
# ─────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# Telegram Bot — standard Bot API (no local server)
# ─────────────────────────────────────────────
_bot_request = HTTPXRequest(
    connection_pool_size=4,
    connect_timeout=30.0,
    read_timeout=120.0,
    write_timeout=120.0,
    pool_timeout=30.0,
)

bot: Bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    request=_bot_request,
)

# ─────────────────────────────────────────────
# Temp directory
# ─────────────────────────────────────────────
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# Arabic Text Cleaning  ← DO NOT MODIFY
# ─────────────────────────────────────────────
KEEP_PATTERN = re.compile(
    r"[^\u0600-\u0605"
    r"\u0610-\u061A"
    r"\u061C"
    r"\u0620-\u063A"
    r"\u0640-\u065F"
    r"\u0670-\u06DC"
    r"\u06DF-\u06E8"
    r"\u06EA-\u06ED"
    r"\uFB50-\uFDFF"
    r"\uFE70-\uFEFF"
    r" ]"
)

def clean_text(raw: str) -> str:
    raw = re.sub(
        r"[\u06DD\u06DE\uFD3E\uFD3F\u0660-\u0669\u06F0-\u06F9()\[\]{}]",
        "", raw,
    )
    raw = KEEP_PATTERN.sub("", raw)
    raw = re.sub(r" +", " ", raw).strip()
    return raw

# ─────────────────────────────────────────────
# Quran Data Loader
# ─────────────────────────────────────────────
def load_quran() -> list[dict]:
    """
    Accepts two JSON formats:

    Format A — flat list:
        [{"surah": 1, "ayah": 1, "text": "..."}, ...]

    Format B — nested dict (your quran_full.json):
        {"1": [{"number": 1, "text": "..."}, ...], "2": [...], ...}
    """
    if not QURAN_JSON_PATH.exists():
        raise RuntimeError(
            "quran_full.json not found. Place it in the repo root."
        )
    with open(QURAN_JSON_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    # Format A — already a flat list
    if isinstance(raw, list):
        return raw

    # Format B — nested dict keyed by surah number string
    flat = []
    for surah_str, ayahs in raw.items():
        for ayah in ayahs:
            flat.append({
                "surah": int(surah_str),
                "ayah":  ayah["number"],
                "text":  ayah["text"],
            })
    flat.sort(key=lambda x: (x["surah"], x["ayah"]))
    log.info(f"Converted nested dict → flat list: {len(flat)} ayahs")
    return flat

QURAN_DATA: list[dict] = []

# ─────────────────────────────────────────────
# App Lifecycle
# ─────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global QURAN_DATA
    QURAN_DATA = load_quran()
    log.info(f"✅ Quran loaded: {len(QURAN_DATA)} ayahs.")
    try:
        me = await bot.get_me()
        log.info(f"✅ Telegram bot ready: @{me.username}")
    except Exception as exc:
        log.error(f"❌ Telegram bot init failed: {exc}")

@app.on_event("shutdown")
async def shutdown():
    log.info("Shutting down.")

# ─────────────────────────────────────────────
# Smart Sampling Logic  ← DO NOT MODIFY
# 40% Full verse | 30% Bridging | 30% Random window
# ─────────────────────────────────────────────
def get_random_task() -> dict:
    roll = random.random()
    idx  = random.randint(0, len(QURAN_DATA) - 1)
    ayah = QURAN_DATA[idx]

    if roll < 0.40:
        mode = "full_verse"
        raw  = ayah["text"]
    elif roll < 0.70:
        mode      = "bridging"
        next_idx  = min(idx + 1, len(QURAN_DATA) - 1)
        next_ayah = QURAN_DATA[next_idx]
        words_curr = ayah["text"].split()
        words_next = next_ayah["text"].split()
        tail = words_curr[max(0, len(words_curr) - 5):]
        head = words_next[:5]
        raw  = " ".join(tail + head)
    else:
        mode  = "random_window"
        words = ayah["text"].split()
        wlen  = len(words)
        win   = min(random.randint(7, 15), wlen)
        start = random.randint(0, max(0, wlen - win))
        raw   = " ".join(words[start : start + win])

    return {
        "task_id": str(uuid.uuid4()),
        "text":    clean_text(raw),
        "mode":    mode,
        "surah":   ayah["surah"],
        "ayah":    ayah["ayah"],
    }

# ─────────────────────────────────────────────
# Batch Size Helper
# ─────────────────────────────────────────────
def get_batch_size() -> int:
    return sum(f.stat().st_size for f in TEMP_DIR.rglob("*") if f.is_file())

# ─────────────────────────────────────────────
# Upload Lock
# ─────────────────────────────────────────────
upload_lock = asyncio.Lock()

# ─────────────────────────────────────────────
# ZIP Chunker
# ─────────────────────────────────────────────
def split_file(src: Path, chunk_size: int) -> list[Path]:
    """
    Split src into ≤chunk_size byte parts.
    Returns list of part paths: src.part001, src.part002, …
    """
    parts   = []
    part_no = 1
    with open(src, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            part_path = src.with_suffix(f".part{part_no:03d}")
            part_path.write_bytes(data)
            parts.append(part_path)
            part_no += 1
    return parts

# ─────────────────────────────────────────────
# Batch Upload
# ─────────────────────────────────────────────
async def upload_batch():
    """
    1. Write metadata.csv  (UTF-8-BOM)
    2. ZIP with STORE compression
    3. Split ZIP into ≤45 MB chunks
    4. Send each chunk as a Telegram document
    5. Log in Supabase
    6. Clear TEMP_DIR
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_path  = Path(f"/tmp/batch_{timestamp}.zip")
    meta_path = TEMP_DIR / "metadata.csv"

    # ── 1. metadata.csv ──────────────────────────────────────────────
    rows: list[dict] = []
    for wav in sorted(TEMP_DIR.glob("*.wav")):
        task_id = wav.stem
        resp = (
            supabase.table("recordings")
            .select("transcript")
            .eq("task_id", task_id)
            .single()
            .execute()
        )
        transcript = resp.data["transcript"] if resp.data else ""
        rows.append({"filename": wav.name, "raw_transcript": transcript})

    with open(meta_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["filename", "raw_transcript"])
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"metadata.csv: {len(rows)} rows")

    # ── 2. ZIP with STORE ─────────────────────────────────────────────
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
        for item in sorted(TEMP_DIR.iterdir()):
            zf.write(item, item.name)

    zip_size_mb = zip_path.stat().st_size / 1_048_576
    log.info(f"ZIP: {zip_path.name} ({zip_size_mb:.1f} MB)")

    # ── 3. Split into ≤45 MB chunks ───────────────────────────────────
    parts      = split_file(zip_path, CHUNK_SIZE_BYTES)
    total_parts = len(parts)
    log.info(f"Split into {total_parts} chunk(s)")

    # ── 4. Upload each chunk ──────────────────────────────────────────
    first_message_id: int | None = None
    try:
        for i, part in enumerate(parts, 1):
            part_mb = part.stat().st_size / 1_048_576

            if i == 1:
                caption = (
                    f"📦 *Batch* `{timestamp}`\n"
                    f"🎙 {len(rows)} recordings | 📁 {zip_size_mb:.1f} MB total\n"
                    f"🗂 Part {i}/{total_parts}"
                )
            else:
                caption = f"🗂 Part {i}/{total_parts} — `{part.name}` ({part_mb:.1f} MB)"

            log.info(f"Uploading part {i}/{total_parts} ({part_mb:.1f} MB)…")

            with open(part, "rb") as pf:
                msg = await bot.send_document(
                    chat_id=TELEGRAM_CHANNEL,
                    document=pf,
                    filename=part.name,
                    caption=caption,
                    parse_mode="Markdown",
                    # Reply to the first chunk so all parts are grouped
                    reply_to_message_id=first_message_id if i > 1 else None,
                    read_timeout=120,
                    write_timeout=120,
                )

            if i == 1:
                first_message_id = msg.message_id

            part.unlink()   # delete chunk after successful upload
            log.info(f"✅ Part {i}/{total_parts} uploaded.")

    except Exception as exc:
        log.error(f"❌ Upload failed on part {i}: {exc} — temp dir preserved for retry.")
        # Clean up any remaining part files
        for p in parts:
            p.unlink(missing_ok=True)
        zip_path.unlink(missing_ok=True)
        return

    # ── 5. Log in Supabase ────────────────────────────────────────────
    supabase.table("batches").insert({
        "batch_name":  zip_path.name,
        "file_count":  len(rows),
        "size_mb":     round(zip_size_mb, 2),
        "parts":       total_parts,
        "uploaded_at": datetime.utcnow().isoformat(),
        "status":      "uploaded",
    }).execute()

    # ── 6. Clear temp dir ─────────────────────────────────────────────
    zip_path.unlink(missing_ok=True)
    shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True)
    log.info("🧹 Temp dir cleared. Ready for next batch.")

# ─────────────────────────────────────────────
# FastAPI Endpoints
# ─────────────────────────────────────────────
@app.get("/get_task")
async def get_task():
    task = get_random_task()
    supabase.table("tasks").insert({
        "task_id": task["task_id"],
        "text":    task["text"],
        "mode":    task["mode"],
        "surah":   task["surah"],
        "ayah":    task["ayah"],
        "status":  "pending",
    }).execute()
    return task


@app.post("/submit")
async def submit(
    task_id:    str        = Form(...),
    transcript: str        = Form(...),
    audio:      UploadFile = File(...),
):
    ALLOWED = {"audio/wav", "audio/wave", "audio/x-wav", "application/octet-stream"}
    if audio.content_type not in ALLOWED:
        raise HTTPException(400, detail=f"Only WAV accepted (got {audio.content_type}).")

    dest = TEMP_DIR / f"{task_id}.wav"
    async with aiofiles.open(dest, "wb") as f:
        await f.write(await audio.read())

    supabase.table("recordings").insert({
        "task_id":    task_id,
        "filename":   dest.name,
        "transcript": transcript,
        "created_at": datetime.utcnow().isoformat(),
    }).execute()
    supabase.table("tasks").update({"status": "recorded"}).eq("task_id", task_id).execute()

    batch_bytes = get_batch_size()
    log.info(f"Batch: {batch_bytes / 1_048_576:.2f} MB")

    if batch_bytes >= BATCH_SIZE_BYTES:
        async with upload_lock:
            if get_batch_size() >= BATCH_SIZE_BYTES:
                asyncio.create_task(upload_batch())

    return JSONResponse({
        "status":   "ok",
        "task_id":  task_id,
        "batch_mb": round(batch_bytes / 1_048_576, 2),
    })


@app.get("/stats")
async def stats():
    batch_bytes = get_batch_size()
    recordings  = supabase.table("recordings").select("task_id", count="exact").execute()
    batches     = (
        supabase.table("batches")
        .select("*")
        .order("uploaded_at", desc=True)
        .execute()
    )
    return {
        "current_batch_mb":   round(batch_bytes / 1_048_576, 2),
        "batch_threshold_mb": BATCH_SIZE_BYTES // 1_048_576,
        "total_recordings":   recordings.count,
        "total_batches":      len(batches.data),
        "batches":            batches.data,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "version": "4.0"}
