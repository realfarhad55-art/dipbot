"""apply_dipbot_handoff_nov26.py - Main bot changes for the separate dipbot.

Dipbot (separate dip-profit bot) takes over dip DETECTION. The main bot
stops detecting dips and only POSTS them via dipbot_import_job, which reads
dipbot's own /opt/dipbot/data/dipbot_queue.json every 1s.

Five edits, two files (run from /opt/bot):

E1 - x_alert.py: disable the _dip_check_tick call site (main bot's own dip
     detection). Normal X milestones are NOT touched.
E2 - x_alert.py: pending_dip_media_job early return (dip retry now lives in
     dipbot: unseal + re-queue). Registration is also removed (E4).
E3 - x_alert.py: add dipbot_import_job + DIPBOT_QUEUE_FILE constant,
     inserted BEFORE `async def monitoring_job(...)`. Exactly-once: SEAL
     FIRST (sent_dip_milestones + _save_tracked), then send_dip_alert.
     Send FAIL -> UNSEAL + clear pending + _save_tracked, so dipbot
     re-queues the level (its stale-entry prune drops the old queue entry
     within 10 min, then the tick fires the level again).
E4 - runtime.py: remove the pending_dip_media_job registration.
E5 - runtime.py: register dipbot_import_job (every 1s, first run 5s).
     Interval is 1s on purpose: polling a local queue file is cheap, and
     it keeps the dip-alert delay at ~1s.

/testdip KEEPS WORKING in the main bot: it calls send_dip_alert directly
(which is NOT touched by this patch) and needs no queue or dipbot.

Rollback: restore the .bak files + restart the main bot, and stop
dipbot.service. Never run dipbot + main-bot dip detection at the same time
(duplicate detection). Also remove DIPBOT_MODE=1 from /opt/monbot/.env
if the monbot dipoff patch was applied.

Idempotent. Backs up x_alert.py to x_alert.py.bak_dipbothandoff_nov26
and runtime.py to runtime.py.bak_dipbothandoff_nov26. All anchors are
pre-verified (count==1) BEFORE anything is written, so a failed anchor
writes nothing at all.
Run: python3 apply_dipbot_handoff_nov26.py [/opt/bot]
Then: systemctl restart bot.service (dipbot must already be running)

WARNING: live /opt/bot/x_alert.py has post-Nov-23 patches (GUARD / FINAL /
PERM / DIPFIX / DIP_BOLD) that are NOT in the Nov 23 base copies this patch
was written against. If any anchor below FAILS on the live files, do NOT
force it - report back instead. Anchors were verified count==1 against the
Nov 23 base copies (x_alert-3aa26821.py, runtime-c97fc7f1.py).
"""

import os, sys, shutil

MARKER = "DIPBOT_HANDOFF_NOV26"

# --- E1: disable the main bot's own dip detection call site ---
OLD1 = '''                # -- DIP PROFIT (DIP_PROFIT_NOV22) --
                # Dip low track karo aur due dip milestones fire karo. Normal
                # X milestones se bilkul alag: apna media, apna template,
                # koi points nahi. Entry gate se guzar chuka hai yahan tak.
                try:
                    await _dip_check_tick(bot, call_key, call, cur_mc)
                except Exception as _e_dip:
                    logger.warning(f"dip check skipped {call_key}: {_e_dip}")'''

NEW1 = '''                # -- DIP PROFIT (DIPBOT_HANDOFF_NOV26) --
                # Dip detection ab dipbot (separate bot) karta hai. Main bot
                # yahan dip check nahi karta. Dipbot ki queue
                # /opt/dipbot/data/dipbot_queue.json se dipbot_import_job
                # import karke post karta hai. /testdip ab bhi yahin kaam
                # karta hai (send_dip_alert untouched).'''

# --- E2: pending_dip_media_job early return ---
OLD2 = '''async def pending_dip_media_job(context):
    try:'''

NEW2 = '''async def pending_dip_media_job(context):
    # DIPBOT_HANDOFF_NOV26: dip retry ab dipbot karta hai (unseal + re-queue).
    # Yeh job band hai; registration bhi runtime.py se hata di gayi hai (E4).
    return
    try:'''

# --- E3: dipbot import job (inserted before monitoring_job) ---
OLD3 = '''async def monitoring_job(context: ContextTypes.DEFAULT_TYPE):'''

NEW3 = '''# DIPBOT_HANDOFF_NOV26: dipbot ki queued dip milestones import karo + post karo.
# Dipbot (separate bot) sirf DETECT karta hai aur apni file
# /opt/dipbot/data/dipbot_queue.json me likhta hai. Yeh job woh file
# har 1 second me READ karke main bot ke tracked_calls / sent_dip_milestones
# se match karta hai, phir send_dip_alert se post karta hai (userbot-first,
# premium emojis). Main bot hi posting + record writes ka malik hai. Dipbot
# kabhi post nahi karta aur uske paas koi Telegram token / userbot session
# nahi hai.
# Exactly-once: SEAL FIRST (sent_dip_milestones + _save_tracked), phir post.
# Send FAIL ho to UNSEAL kar do (seal hatao + pending clear + _save_tracked)
# taake dipbot dobara queue kare (uski stale-entry prune 10 min me purani
# entry hata deti hai, phir tick dobara fire karta hai). Pending retry job
# (pending_dip_media_job) band hai, isliye unseal hi retry ka rasta hai.
DIPBOT_QUEUE_FILE = "/opt/dipbot/data/dipbot_queue.json"


async def dipbot_import_job(context):
    import json
    try:
        with open(DIPBOT_QUEUE_FILE, encoding="utf-8") as f:
            queue = json.load(f) or {}
    except FileNotFoundError:
        return  # dipbot abhi start nahi hua
    except Exception as e:
        logger.warning(f"dipbot_import: read failed: {e}")
        return
    if not isinstance(queue, dict) or not queue:
        return
    try:
        entries = sorted(queue.values(), key=lambda e: str(e.get("detected_at", "")))
    except Exception:
        entries = list(queue.values())
    for entry in entries[:10]:  # max 10 per run, baaki agle second me
        try:
            if not isinstance(entry, dict):
                continue
            call_key = entry.get("call_key", "")
            x_val = int(entry.get("x_val", 0) or 0)
            call = tracked_calls.get(call_key)
            if not call or x_val <= 0:
                continue
            sent_dip = call.get("sent_dip_milestones")
            if not isinstance(sent_dip, list):
                sent_dip = []
                call["sent_dip_milestones"] = sent_dip
            if x_val in sent_dip:
                continue  # already posted (sealed)
            try:
                call["dip_low"] = float(entry.get("dip_low", 0) or 0)
            except (TypeError, ValueError):
                pass
            sent_dip.append(x_val)
            _save_tracked()
            try:
                ok = await send_dip_alert(context.bot, call_key, call, x_val,
                                          float(entry.get("cur_mc", 0) or 0))
            except Exception as e:
                logger.warning(f"dipbot_import: dip send failed {x_val}X {call_key}: {e}")
                ok = False
            if ok:
                logger.info(f"dipbot_import: dip posted {x_val}X {call_key}")
            else:
                # Send fail: UNSEAL taake dipbot dobara queue kare. Pending
                # retry job band hai, isliye yeh hi retry ka rasta hai.
                # send_dip_alert ne pending_dip me daal diya hoga; use bhi
                # clear karo taake do raste na banein.
                try:
                    if x_val in sent_dip:
                        sent_dip.remove(x_val)
                except (TypeError, ValueError):
                    pass
                try:
                    _dip_clear_pending(call_key, x_val)
                except Exception:
                    pass
                _save_tracked()
                logger.info(f"dipbot_import: dip held {x_val}X {call_key} (unsealed, dipbot will re-queue)")
        except Exception as e:
            logger.warning(f"dipbot_import: skip entry: {e}")
            continue


async def monitoring_job(context: ContextTypes.DEFAULT_TYPE):'''

# --- E4: remove pending_dip_media_job registration ---
OLD4 = '''    app.job_queue.run_repeating(pending_dip_media_job, interval=15,        first=25)   # DIP_PROFIT_NOV22: release held dip alerts once media is uploaded'''

NEW4 = '''    # DIPBOT_HANDOFF_NOV26: pending_dip_media_job band hai (dip retry ab dipbot karta hai).'''

# --- E5: register dipbot_import_job (after userbot_watchdog_job) ---
OLD5 = '''    app.job_queue.run_repeating(userbot_watchdog_job, interval=45,        first=45)   # har 45s: userbot alive check + auto-reconnect (backstop for the immediate _kick_userbot_reconnect)'''

NEW5 = '''    app.job_queue.run_repeating(userbot_watchdog_job, interval=45,        first=45)   # har 45s: userbot alive check + auto-reconnect (backstop for the immediate _kick_userbot_reconnect)
    app.job_queue.run_repeating(dipbot_import_job, interval=1, first=5)  # DIPBOT_HANDOFF_NOV26: dipbot ki dip queue import karo + post karo (1s: local file read cheap hai, dip alert fast rehta hai)'''


def check_anchors(xasrc, rtsrc):
    """Pre-verify every anchor (count==1) before writing anything."""
    checks = [
        ("E1", xasrc, OLD1),
        ("E2", xasrc, OLD2),
        ("E3", xasrc, OLD3),
        ("E4", rtsrc, OLD4),
        ("E5", rtsrc, OLD5),
    ]
    for tag, src, old in checks:
        n = src.count(old)
        if n != 1:
            print("FAIL: %s anchor missing/dup (found %dx) -- NOT writing "
                  "anything." % (tag, n))
            return False
    return True


def patch_one(path, old, new, tag):
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    bak = path + ".bak_dipbothandoff_nov26"
    if not os.path.exists(bak):
        # First edit only: keep a PRISTINE pre-patch backup (later edits
        # must not overwrite it, or rollback would restore a half-patched file).
        shutil.copy2(path, bak)
    src = src.replace(old, new, 1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src)
    return True


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    xa = os.path.join(d, "x_alert.py")
    rt = os.path.join(d, "runtime.py")
    for p in (xa, rt):
        if not os.path.exists(p):
            print("FAIL: %s not found" % p)
            return 1
    with open(xa, encoding="utf-8") as f:
        xasrc = f.read()
    with open(rt, encoding="utf-8") as f:
        rtsrc = f.read()
    if MARKER in xasrc and "dipbot_import_job" in xasrc and MARKER in rtsrc:
        print("OK: already has DIPBOT_HANDOFF_NOV26, skipping.")
        return 0
    if MARKER in xasrc or "dipbot_import_job" in xasrc or MARKER in rtsrc:
        print("FAIL: partial state (marker in one file only) -- inspect "
              "x_alert.py / runtime.py before re-running.")
        return 1
    if not check_anchors(xasrc, rtsrc):
        return 1
    patch_one(xa, OLD1, NEW1, "E1")
    print("E1 OK (main bot dip detection OFF)")
    patch_one(xa, OLD2, NEW2, "E2")
    print("E2 OK (pending_dip_media_job no-op)")
    patch_one(xa, OLD3, NEW3, "E3")
    print("E3 OK (dipbot_import_job added to x_alert.py)")
    patch_one(rt, OLD4, NEW4, "E4")
    print("E4 OK (pending_dip_media_job registration removed)")
    patch_one(rt, OLD5, NEW5, "E5")
    print("E5 OK (dipbot_import_job registered in runtime.py, every 1s)")
    print("DONE. Backups: x_alert.py.bak_dipbothandoff_nov26, runtime.py.bak_dipbothandoff_nov26")
    print("Next: systemctl restart bot.service (dipbot must already be running)")
    print("Verify: /testdip 5 in the bot DM still works; a real dip alert posts with media + premium emojis.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
