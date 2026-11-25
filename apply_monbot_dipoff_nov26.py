"""apply_monbot_dipoff_nov26.py - Monbot changes for the separate dipbot.

Dipbot (separate dip-profit bot) takes over dip DETECTION from monbot.
Monbot keeps X-milestone detection; it just stops detecting dips.

One edit, one file (run from /opt/monbot):

E1 - monbot.py: _monbot_dip_tick early return when DIPBOT_MODE=1.
     The call site is left alone (cheap no-op call). Rollback is just
     removing DIPBOT_MODE=1 from /opt/monbot/.env + restarting monbot.

Why an env gate instead of deleting the code: the dip engine stays in
monbot.py as a fallback. If dipbot ever goes down for a long time, the
user can turn monbot dip detection back on with one env change (and stop
dipbot first, so the two never detect at the same time).

Idempotent. Backs up monbot.py to monbot.py.bak_dipbotdipoff_nov26.
Anchor is pre-verified (count==1) before anything is written.
Run: python3 apply_monbot_dipoff_nov26.py [/opt/monbot]
Then: echo DIPBOT_MODE=1 >> /opt/monbot/.env && systemctl restart monbot.service

WARNING: this patch was written against the monbot.py delivered Nov 25
(132,097 B). If the anchor below FAILS on the live file, do NOT force it -
report back instead.
"""

import os, sys, shutil

MARKER = "DIPBOT_DIPOFF_NOV26"

# --- E1: _monbot_dip_tick early return when DIPBOT_MODE=1 ---
OLD1 = '''async def _monbot_dip_tick(call_key, call, cur_mc):
    if not dip_enabled():
        return'''

NEW1 = '''async def _monbot_dip_tick(call_key, call, cur_mc):
    # DIPBOT_DIPOFF_NOV26: dip detection moved to the separate dipbot.
    # Monbot keeps X milestones only. Set DIPBOT_MODE=1 in /opt/monbot/.env.
    if os.environ.get("DIPBOT_MODE") == "1":
        return
    if not dip_enabled():
        return'''


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "."
    mp = os.path.join(d, "monbot.py")
    if not os.path.exists(mp):
        print("FAIL: %s not found" % mp)
        return 1
    with open(mp, encoding="utf-8") as f:
        src = f.read()
    if MARKER in src:
        print("OK: already has DIPBOT_DIPOFF_NOV26, skipping.")
        return 0
    if src.count(OLD1) != 1:
        print("FAIL: E1 anchor missing/dup (found %dx) -- NOT writing "
              "anything." % src.count(OLD1))
        return 1
    bak = mp + ".bak_dipbotdipoff_nov26"
    if not os.path.exists(bak):
        shutil.copy2(mp, bak)
    src = src.replace(OLD1, NEW1, 1)
    with open(mp, "w", encoding="utf-8") as f:
        f.write(src)
    print("E1 OK (_monbot_dip_tick OFF when DIPBOT_MODE=1)")
    print("DONE. Backup: monbot.py.bak_dipbotdipoff_nov26")
    print("Next: echo DIPBOT_MODE=1 >> /opt/monbot/.env && systemctl restart monbot.service")
    print("Verify: monbot still queues X milestones (monbot_milestones.json grows), but no new dip entries appear there.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
