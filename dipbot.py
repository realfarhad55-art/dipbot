#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""dipbot.py - WizardScan DIP-PROFIT alert bot (FOURTH bot, standalone).

This bot is SEPARATE from the main wizu bot, from monbot (X detection) and
from dropbot (dropped calls). It does ONE job: watch the main bot's tracked
calls for DIP-PROFIT milestones and queue them for the main bot to post.

DESIGN (same safe pattern as monbot, Nov 24 2026):
- The main bot runs at 95-156% CPU. Its own dip detection lived inside the
  monitoring path (check_one -> _dip_check_tick) and monbot's dip half
  (_monbot_dip_tick) ran inside the X monitor. This bot takes over dip
  detection entirely, so monbot becomes X-only and the main bot does no dip
  detection at all.
- This bot reads the main bot's data READ-ONLY:
  /opt/bot/data/tracked_calls.json, bot_config.json, channels.json
  (+ priority_channels.json if present). It NEVER writes them.
- Dip state lives in this bot's OWN file only:
  /opt/dipbot/data/dipbot_dip.json (dip_low, cycle low, sent/pending levels).
  Ready dip posts go to its OWN queue file:
  /opt/dipbot/data/dipbot_queue.json (same entry shape as monbot's queue,
  kind="dip", with dip_low + cur_mc).
- The main bot's dipbot_import_job (added by apply_dipbot_handoff_nov26.py)
  reads that queue every 2s and POSTS the dip alert itself, then forwards it
  to the KOL owner. The main bot owns ALL posting and ALL record writes.
- This bot needs NO bot token and NO userbot session. Do NOT add Telegram
  credentials here.

POSTING PATH - WHY A RELAY (read this before "fixing" it):
- Dip alerts carry media (video) with premium custom emojis in the caption.
  Proven fact: the Bot API STRIPS custom-emoji entities from media captions
  sent to a channel, so a Bot-API post renders NON-premium (plain crystal
  balls). The userbot (MTProto, @yowziuh) is the ONLY proven path for
  premium emojis in channel posts.
- A userbot session allows only ONE live connection. The main bot holds
  @yowziuh and the dropbot owns @Wizard_Scan exclusively (sharing a session
  between two bots disconnects one of them - that is what hit dropbot on
  Nov 24). So dipbot CANNOT hold a userbot session of its own without
  either (a) flapping the main bot's session (connect-per-post kicks the
  main bot off every time, and risks flood limits), or (b) a THIRD Telegram
  account (new phone number + Premium + DIP pack install - a user action
  that blocks deploy).
- So dipbot QUEUES ready posts and the main bot EXECUTES the send via its
  existing userbot-first path (premium emojis kept, zero session risk, zero
  new account). Tradeoff, stated plainly: the main bot still spends ~1
  send_video + 1 forward per dip alert (I/O-bound, ~1s, negligible CPU).
  Everything expensive - price fetching for 3,300+ calls, low tracking,
  milestone math, media-hold retries - moves to dipbot. That is the real
  load reduction. A direct-post backend can be added later if the user
  provides a dedicated dipbot session; until then the relay is the shipped
  path and Bot-API direct posting stays OFF (it would silently lose the
  premium emojis the user cares about).

STATE SAFETY (hard rule):
- No open(..., "w") and no _save_tracked / _save_milestones calls targeting
  main bot files anywhere in this file. The only files this bot writes are
  under DIPBOT_DATA_DIR: dipbot_queue.json, dipbot_dip.json.
- Call dicts are this bot's PRIVATE deep copies. Mutating them cannot leak
  anywhere, because the main bot's files are only ever read, never written.
- sent_dip_milestones in dipbot's private copies is ALWAYS the main bot's
  sealed list (restored from the file on every reload). It is the source of
  truth for "posted". Dipbot never marks a level sent at queue time - a
  level counts as sent only once the main bot sealed it. If a post fails,
  the main bot's drain unseals it and dipbot re-queues it (exactly-once).

DIP RULES (final, do NOT change - owner, Nov 22-23):
- dip_low = lowest MC after entry. Dip X = current MC / dip_low.
- Min dip 50%, max dip 99% (threshold via /setdipthreshold, stored config
  governs; dip_threshold_pct() clamps to [1.0, 50.0]).
- Dip low floor = max(1000.0, entry_mc * 0.01); low confirm = 1 tick.
- Multi-dip: dip_low only goes down; a new confirmed low >=20% below the
  previous cycle low resets sent_dip_milestones + pending_dip (cycle restarts).
- Milestones: 2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50,60,70,80,90,100,150,
  200,250,500,1000,1500, then every +500 (cap 10000).
- Media hold: no media set for a level = HOLD/park (pending_dip, retried
  every 15s), NEVER text-only. Dip alerts award NO normal points.
- Delivery: dip alert posts to @WizardScan FIRST (userbot rich send, then
  Bot API explicit-entities, then Bot API HTML), then the channel post is
  FORWARDED to the KOL owner ONLY, gated by the owner's saved dip level
  choice (default All) AND the channel's dip choice (DM Alert Settings
  patch 4). That gating lives in the main bot and runs at post time.
- Titles, caption template, emoji IDs: exactly as set by the owner. The
  main bot builds the caption at post time (send_dip_alert); dipbot only
  logs the title per queued level. See dipbot-deploy.md for the template.

ENV: .env file next to this script (see dipbot.env.example). No token or
session vars exist here on purpose.
"""

# --- imports (same block as monbot.py - the extracted code needs them) ---
import os
import sys
import re
import io
import json
import time
import copy
import signal
import logging
import requests
import asyncio
import functools
import threading
import contextvars as _contextvars
import time as _time
import threading as _threading
import asyncio as _asyncio
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S")
def _load_dotenv(path):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass


logger = logging.getLogger("dipbot")


DIPBOT_DATA_DIR = os.environ.get("DIPBOT_DATA_DIR", "/opt/dipbot/data")


DIPBOT_MAIN_DATA_DIR = os.environ.get("DIPBOT_MAIN_DATA_DIR", "/opt/bot/data")


DIPBOT_QUEUE_FILE = os.environ.get(
    "DIPBOT_QUEUE_FILE", os.path.join(DIPBOT_DATA_DIR, "dipbot_queue.json"))


DIPBOT_DIP_FILE = os.environ.get(
    "DIPBOT_DIP_FILE", os.path.join(DIPBOT_DATA_DIR, "dipbot_dip.json"))


MAIN_CONFIG_FILE = os.path.join(DIPBOT_MAIN_DATA_DIR, "bot_config.json")


MAIN_CHANNELS_FILE = os.path.join(DIPBOT_MAIN_DATA_DIR, "channels.json")


MAIN_PRIO_FILE = os.path.join(DIPBOT_MAIN_DATA_DIR, "priority_channels.json")


FALLBACK_CONFIG_FILE = os.path.join(DIPBOT_DATA_DIR, "bot_config.json")


FALLBACK_CHANNELS_FILE = os.path.join(DIPBOT_DATA_DIR, "channels.json")


FALLBACK_PRIO_FILE = os.path.join(DIPBOT_DATA_DIR, "priority_channels.json")


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_json_file(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if data is not None else default
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.warning(f"dipbot: read failed {path}: {e}")
        return default


_config_cache = {"mtime": 0.0, "data": {}}


_channels_cache = {"mtime": 0.0, "data": []}


_prio_cache = {"mtime": 0.0, "data": set()}


def load_config():
    for path in (MAIN_CONFIG_FILE, FALLBACK_CONFIG_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _config_cache["mtime"] or _config_cache.get("path") != path:
            _config_cache["data"] = _read_json_file(path, {}) or {}
            _config_cache["mtime"] = mtime
            _config_cache["path"] = path
        return _config_cache["data"]
    return dict(_config_cache["data"])


def load_channels():
    for path in (MAIN_CHANNELS_FILE, FALLBACK_CHANNELS_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _channels_cache["mtime"] or _channels_cache.get("path") != path:
            data = _read_json_file(path, []) or []
            if isinstance(data, dict):
                data = list(data.keys())
            _channels_cache["data"] = list(data)
            _channels_cache["mtime"] = mtime
            _channels_cache["path"] = path
        return list(_channels_cache["data"])
    return list(_channels_cache["data"])


def priority_channels_lower():
    for path in (MAIN_PRIO_FILE, FALLBACK_PRIO_FILE):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime != _prio_cache["mtime"] or _prio_cache.get("path") != path:
            data = _read_json_file(path, []) or []
            if isinstance(data, dict):
                data = list(data.keys())
            _prio_cache["data"] = {str(c).lower() for c in data}
            _prio_cache["mtime"] = mtime
            _prio_cache["path"] = path
        return set(_prio_cache["data"])
    return set(_prio_cache["data"])


X_MILESTONES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22, 25, 28,
                30, 32, 34, 35, 38, 40, 45, 50,
                55, 60, 65, 75, 100, 150, 200, 250, 300,  # SUPPRESS_LEVELS_NOV14: 70 removed - no 70x alerts
                500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000,
                6000, 7000, 8000, 9000, 10000,
                11000, 12000, 13000, 14000, 15000, 20000, 25000, 30000,
                40000, 50000, 60000, 70000, 80000, 90000, 100000]


MAX_MILESTONE = 1_000


SUPPORTED_CHAINS = {"ethereum": "ETH", "bsc": "BNB", "base": "BASE", "solana": "SOL",
                    "robinhood": "RH", "ton": "TON", "arc": "ARC"}


CHAIN_TO_DEXPATH = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base",
                    "RH": "robinhood", "TON": "ton", "ARC": "arc"}


def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return default


def get_milestones():
    cfg = load_config()
    stored = cfg.get("custom_milestones")
    if stored:
        try: base = sorted(set(int(x) for x in stored))
        except Exception: base = list(X_MILESTONES)
    else:
        base = list(X_MILESTONES)
    # Also include X values for which milestone_media is set, so those X levels are always tracked
    try:
        media_keys = cfg.get("milestone_media", {}).keys()
        extra = [int(k) for k in media_keys if k.lstrip('-').isdigit() and int(k) <= MAX_MILESTONE]
        if extra:
            base = sorted(set(base) | set(extra))
    except Exception:
        pass
    return base


def fmt_mc(value):
    """Format market cap without a currency sign, e.g. 12.5K MC or 3.45M MC."""
    if not value: return "N/A"
    if value >= 1_000_000_000: return f"{value/1_000_000_000:.2f}B MC"
    elif value >= 1_000_000:   return f"{value/1_000_000:.2f}M MC"
    elif value >= 1_000:       return f"{value/1_000:.1f}K MC"
    return f"{value:.0f} MC"


MC_PRICE_PUMP_GUARD = os.environ.get("MC_PRICE_PUMP_GUARD", "1").strip().lower() not in ("0", "false", "off")


_dex_cache: dict = {}


_DEX_CACHE_TTL = float(os.environ.get("DEX_CACHE_TTL_SECONDS", "2.5") or 2.5)


def _pick_market_cap(pairs: list) -> tuple[float, bool]:
    """Return (market_cap, is_fdv) - the most trustworthy market cap from a
    DexScreener pairs list, and whether it had to fall back to FDV.

    DexScreener reports marketCap/fdv per pair and a single stale pair can be
    badly wrong. We therefore:
      1. take marketCap from the deepest-liquidity pair,
      2. cross-check it against the median marketCap of all pairs of the same
         token - if it is more than 3x off, trust the median instead,
      3. never substitute FDV for real market cap AS IF it were real.

    FIX (RH-chain "called at 19K, bot shows 119K instantly" incident,
    2026-09-24): point 3 used to mean "return 0.0" whenever no pair had a
    real marketCap field - which DexScreener commonly never populates for
    Robinhood-chain (tokenized-stock) pairs, only `fdv`. A silent 0.0 here
    means _reconcile_entry_mc sees "nothing to compare the caller's post-hint
    against" and blindly LOCKS IN that hint as fully "confirmed", with the
    entire cross-check/correction machinery (_confirm_entry_mc, the pre-post
    settle check, the _mc_is_fdv ratio guard) never engaging at all - because
    all of that machinery only ever triggers off an explicit _mc_is_fdv flag,
    and this path never set one. A caller posting (or a bot mis-parsing) a
    lowballed "called at 2.7K" MC then rode straight through to a real 65K
    chart as a fake "24X", with zero of the anti-cheat checks built for
    exactly this scenario ever firing.
    Now: FDV is still never trusted AS real MC (nothing changes for the
    normal case), but it IS returned as a flagged, provisional number instead
    of a bare 0 - so callers correctly route it through the same
    live_fdv/post_unconfirmed provisional path already built for alt sources
    (Birdeye/GeckoTerminal), which locks it in only after an independent
    cross-check confirms it.
    """
    mcs = []
    for p in pairs:
        try:
            v = float(p.get("marketCap") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            mcs.append(v)
    if mcs:
        primary = mcs[0]
        if len(mcs) >= 3:
            ordered = sorted(mcs)
            median = ordered[len(ordered) // 2]
            if median > 0 and (primary > median * 3 or primary * 3 < median):
                return (median, False)
        return (primary, False)
    # No pair has a real circulating marketCap - fall back to the
    # deepest-liquidity pair's FDV, flagged, rather than a bare 0.0.
    for p in sorted(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0,
                     reverse=True):
        try:
            fdv = float(p.get("fdv") or 0)
        except (TypeError, ValueError):
            fdv = 0.0
        if fdv > 0:
            return (fdv, True)
    return (0.0, False)


def _parse_dex_pairs(pairs: list, min_liquidity: int, token_address: str = "", prefer_chain: str = None) -> dict | None:
    """Parse a DexScreener pairs list into our internal dict. Returns None if unusable."""
    sup = [p for p in pairs if p.get("chainId","").lower() in SUPPORTED_CHAINS
           and (p.get("liquidity",{}).get("usd") or 0) >= min_liquidity]
    # priceUsd always describes baseToken.  A searched contract can also occur
    # as quoteToken; treating that pair as the call token silently tracks the
    # other asset (often SOL/WETH) and leaves the call stuck around 1x.
    if token_address:
        wanted = token_address.lower()
        sup = [p for p in sup if ((p.get("baseToken") or {}).get("address") or "").lower() == wanted]
    # WIZCHAIN_NOV23: when the caller knows the chain (e.g. the KOL's pair
    # link was on BSC), only pairs on that chain count. A multi-chain token
    # must not resolve to a different chain's pair (newest-pair guard /
    # ETH-first lookup). Safe fallback: if no pair matches, sup is unchanged.
    if prefer_chain:
        _pc = str(prefer_chain).lower()
        _on = [p for p in sup if (p.get("chainId") or "").lower() == _pc]
        if _on:
            sup = _on
    if not sup:
        return None
    # Cross-chain address collision guard: the exact same literal address can
    # exist as a completely unrelated pair on more than one EVM chain (vanity/
    # CREATE2 deployments, or plain coincidence). Sorting by liquidity alone
    # in that case can silently lock onto a totally different, much bigger
    # project on another chain - e.g. a fresh 435 MC KOL call getting matched
    # to an unrelated $1.5M pool elsewhere that happens to share this address.
    # A freshly-posted call is virtually always the NEWEST pool, so when
    # multiple different chains match, narrow down to the chain whose pool
    # was created most recently before ranking by liquidity within it.
    chains_seen = {p.get("chainId", "").lower() for p in sup}
    if len(chains_seen) > 1:
        dated = [p for p in sup if p.get("pairCreatedAt")]
        if dated:
            newest_chain = max(dated, key=lambda p: p.get("pairCreatedAt") or 0).get("chainId", "").lower()
            same_chain = [p for p in sup if p.get("chainId", "").lower() == newest_chain]
            if same_chain and len(same_chain) < len(sup):
                logger.warning(
                    f"⚠️ Cross-chain address collision for {token_address[:14]}… "
                    f"across {sorted(chains_seen)} - restricting to newest pool's "
                    f"chain ({newest_chain}) to avoid tracking the wrong project")
                sup = same_chain
    sup = sorted(sup, key=lambda p: p.get("liquidity",{}).get("usd",0) or 0, reverse=True)
    best = sup[0]
    chain  = SUPPORTED_CHAINS[best.get("chainId","").lower()]
    price  = float(best.get("priceUsd") or 0)
    mc, mc_is_fdv = _pick_market_cap(sup)
    symbol = best.get("baseToken",{}).get("symbol","")
    if price <= 0 and mc <= 0 and not symbol:
        return None
    price_change_h1  = float((best.get("priceChange") or {}).get("h1") or 0)
    price_change_h24 = float((best.get("priceChange") or {}).get("h24") or 0)
    volume_h24       = float((best.get("volume") or {}).get("h24") or 0)
    liquidity_usd    = float((best.get("liquidity") or {}).get("usd") or 0)
    txns_h24_buys    = int((best.get("txns") or {}).get("h24", {}).get("buys") or 0)
    txns_h24_sells   = int((best.get("txns") or {}).get("h24", {}).get("sells") or 0)
    listed_at        = best.get("pairCreatedAt")
    socials = best.get("info",{}).get("socials") or []
    tg_link = ""
    for s in socials:
        if "t.me" in (s.get("url","")) or "telegram" in (s.get("type","")).lower():
            tg_link = s.get("url",""); break
    return {
        "chain":           chain,
        "mcap":            mc,
        "mcap_fmt":        fmt_mc(mc) if mc > 0 else "N/A",
        "price":           price,
        "symbol":          symbol,
        "name":            best.get("baseToken",{}).get("name",""),
        "pair_addr":       best.get("pairAddress",""),
        "tg_link":         tg_link,
        "price_change_h1": price_change_h1,
        "price_change_h24":price_change_h24,
        "volume_h24":      volume_h24,
        "liquidity_usd":   liquidity_usd,
        "txns_buys":       txns_h24_buys,
        "txns_sells":      txns_h24_sells,
        "listed_at":       listed_at,
        "_partial":        (price <= 0 and mc <= 0),
        "_mc_is_fdv":      bool(mc_is_fdv and mc > 0),
    }


_dex_session = requests.Session()


_dex_miss_cache: dict = {}


_DEX_CACHE_MAX = 4000


_DEX_429_UNTIL = 0.0


_DEX_429_COOLDOWN = int(os.environ.get("DEX_429_COOLDOWN_SECONDS", "20") or 20)


def _dex_breaker_open() -> bool:
    return time.time() < _DEX_429_UNTIL


def _dex_trip_breaker():
    global _DEX_429_UNTIL
    _DEX_429_UNTIL = time.time() + _DEX_429_COOLDOWN


_DEX_RATE_LOCK = _threading.Lock()


_DEX_RATE_TIMESTAMPS = deque()


_DEX_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("DEX_MAX_REQUESTS_PER_MINUTE", "260") or 260))


_DEX_STATS = {"ok": 0, "real429": 0, "budget_denied": 0, "breaker_denied": 0, "neterr": 0,
              "bulk_ok": 0, "single_ok": 0, "mon_ok": 0,
              "bulk_denied": 0, "single_denied": 0, "mon_denied": 0}


_DEX_STATS_AT = time.time()


_DEX_EFF_BUDGET = float(_DEX_MAX_REQUESTS_PER_MINUTE)


_DEX_LAST_429_AT = 0.0


_DEX_LAST_DEC_AT = 0.0


_DEX_LAST_ADJ_AT = time.time()


_DEX_BUDGET_FLOOR = 60


_DEX_ADAPTIVE = (os.environ.get("DEX_ADAPTIVE_BUDGET", "0") or "0").strip() == "1"


def _dex_stat(key: str):
    _DEX_STATS[key] = _DEX_STATS.get(key, 0) + 1


def _dex_stats_maybe_log():
    global _DEX_STATS_AT
    now = time.time()
    if now - _DEX_STATS_AT < 60:
        return
    snap = dict(_DEX_STATS)
    for k in list(_DEX_STATS):
        _DEX_STATS[k] = 0
    _DEX_STATS_AT = now
    logger.info(
        f"📊 DexScreener last min: ok={snap.get('ok',0)} real_429={snap.get('real429',0)} "
        f"budget_denied={snap.get('budget_denied',0)} breaker_denied={snap.get('breaker_denied',0)} "
        f"net_err={snap.get('neterr',0)} | budget={int(_DEX_EFF_BUDGET)}/{_DEX_MAX_REQUESTS_PER_MINUTE} "
        f"| lanes ok bulk={snap.get('bulk_ok',0)} single={snap.get('single_ok',0)} mon={snap.get('mon_ok',0)} "
        f"denied bulk={snap.get('bulk_denied',0)} single={snap.get('single_denied',0)} mon={snap.get('mon_denied',0)} "
        f"cooldown={_DEX_429_COOLDOWN}s adaptive={'on' if _DEX_ADAPTIVE else 'off'}")


def _dex_effective_budget() -> int:
    global _DEX_EFF_BUDGET, _DEX_LAST_ADJ_AT
    now = time.time()
    if (_DEX_EFF_BUDGET < _DEX_MAX_REQUESTS_PER_MINUTE
            and now - _DEX_LAST_429_AT > 60 and now - _DEX_LAST_ADJ_AT > 30):
        _DEX_EFF_BUDGET = min(float(_DEX_MAX_REQUESTS_PER_MINUTE),
                              _DEX_EFF_BUDGET + max(5.0, _DEX_MAX_REQUESTS_PER_MINUTE * 0.05))
        _DEX_LAST_ADJ_AT = now
    return int(_DEX_EFF_BUDGET)


def _dex_on_real_429():
    """Called when DexScreener itself returned HTTP 429."""
    global _DEX_EFF_BUDGET, _DEX_LAST_429_AT, _DEX_LAST_DEC_AT
    now = time.time()
    _DEX_LAST_429_AT = now
    if not _DEX_ADAPTIVE:
        return   # observe only - never lowers the request budget
    # several in-flight requests can 429 together - cut only once per 10s
    if now - _DEX_LAST_DEC_AT >= 10:
        _DEX_LAST_DEC_AT = now
        _DEX_EFF_BUDGET = max(float(_DEX_BUDGET_FLOOR), _DEX_EFF_BUDGET * 0.8)


_DEX_LANE_CV = _contextvars.ContextVar("dex_lane", default="single")


_DEX_LANE_TS = {"bulk": deque(), "single": deque(), "mon": deque()}


_DEX_MON_SHARE = float(os.environ.get("DEX_MON_SHARE", "0.25") or 0.25)


def _dex_rate_budget_ok(lane: str = None) -> bool:
    now = time.time()
    lane = lane or _DEX_LANE_CV.get()
    if lane not in _DEX_LANE_TS:
        lane = "single"
    budget = _dex_effective_budget()
    with _DEX_RATE_LOCK:
        for dq in _DEX_LANE_TS.values():
            while dq and now - dq[0] > 60:
                dq.popleft()
        total = sum(len(dq) for dq in _DEX_LANE_TS.values())
        if total >= budget:
            _dex_stat(lane + "_denied")
            return False
        # naye call ka instant track ("single" lane) ke liye ~12% budget HAMESHA reserved:
        # bulk/mon lanes use kabhi nahi chhoo sakte, taake dropped-call speed par asar na pade.
        if lane in ("bulk", "mon") and total >= budget - max(40, int(budget * 0.14)):   # 2026-10-26: 40 slots naye call ke liye reserved
            _dex_stat(lane + "_denied")
            return False
        if lane == "mon" and len(_DEX_LANE_TS["mon"]) >= max(8, int(budget * _DEX_MON_SHARE)):
            _dex_stat("mon_denied")
            return False
        _DEX_LANE_TS[lane].append(now)
        _DEX_RATE_TIMESTAMPS.append(now)          # purana counter (compat)
        while _DEX_RATE_TIMESTAMPS and now - _DEX_RATE_TIMESTAMPS[0] > 60:
            _DEX_RATE_TIMESTAMPS.popleft()
        _dex_stat(lane + "_ok")
        return True


_BIRDEYE_RATE_LOCK = _threading.Lock()


_BIRDEYE_RATE_TIMESTAMPS = deque()


_BIRDEYE_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("BIRDEYE_MAX_REQUESTS_PER_MINUTE", "50") or 50))


def _birdeye_rate_budget_ok() -> bool:
    now = time.time()
    with _BIRDEYE_RATE_LOCK:
        while _BIRDEYE_RATE_TIMESTAMPS and now - _BIRDEYE_RATE_TIMESTAMPS[0] > 60:
            _BIRDEYE_RATE_TIMESTAMPS.popleft()
        if len(_BIRDEYE_RATE_TIMESTAMPS) >= _BIRDEYE_MAX_REQUESTS_PER_MINUTE:
            return False
        _BIRDEYE_RATE_TIMESTAMPS.append(now)
        return True


def _dex_cache_gc():
    """Evict expired / overflowing cache entries (prevents unbounded RAM growth)."""
    import time as _time
    now = _time.time()
    for cache in (_dex_cache, _dex_miss_cache):
        if len(cache) < _DEX_CACHE_MAX:
            continue
        for k in [k for k, v in list(cache.items())
                  if (v[1] if isinstance(v, tuple) else v) < now]:
            cache.pop(k, None)
        while len(cache) > _DEX_CACHE_MAX:
            try:
                cache.pop(next(iter(cache)))
            except StopIteration:
                break


def _dex_get(url: str, timeout: int = 5, fresh: bool = False, lane: str = None):
    """GET with shared session. fresh=True bypasses intermediary HTTP caches."""
    try:
        _dex_stats_maybe_log()
    except Exception:
        pass
    if _dex_breaker_open():
        _dex_stat("breaker_denied")
        return 429  # breaker open: request bhejne ke baghair rate-limited batao
    if not _dex_rate_budget_ok(lane):
        _dex_stat("budget_denied")
        return 429  # apna hi budget khatam - DexScreener ko hit kiye baghair fallback pe jao
    try:
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"} if fresh else None
        if fresh:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}_ts={int(time.time() * 1000)}"
        resp = _dex_session.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        _dex_stat("neterr")
        logger.warning(f"DexScreener network error ({url.split('/')[-1][:16]}…): {e}")
        return None
    if resp.status_code == 429:
        _dex_stat("real429")
        _dex_on_real_429()
        _dex_trip_breaker()
        return 429
    if resp.status_code != 200:
        return None
    _dex_stat("ok")
    try:
        return resp.json()
    except Exception:
        return None


def _dex_candidate_chains(ca: str) -> list:
    """Which DexScreener chains can this address exist on? Keeps the
    chain-scoped lookup to 1-4 cheap requests instead of brute-forcing."""
    # len==42: standard EVM (0x+40hex). len==66: Robinhood/"RH" 32-byte
    # address format (0x+64hex) seen on DexScreener chart links - without
    # this, a correctly-extracted Robinhood CA still failed here and fell
    # through to being queried as if it were a Solana address.
    if ca.startswith("0x") and len(ca) in (42, 66):
        return ["ethereum", "bsc", "base", "robinhood"]
    if ca.startswith(("EQ", "UQ")):
        return ["ton"]
    return ["solana"]


BIRDEYE_API_KEY = os.environ.get("BIRDEYE_API_KEY", "").strip()


MORALIS_API_KEY = os.environ.get("MORALIS_API_KEY", "").strip()


CMC_API_KEY     = (os.environ.get("CMC_API_KEY", "")
                   or os.environ.get("COINMARKETCAP_API_KEY", "")).strip()


EXTRA_SOURCES_ENABLED = os.environ.get("EXTRA_SOURCES", "1").strip().lower() not in ("0", "false", "off")


_GT_NET   = {"SOL": "solana", "ETH": "eth", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}


_DP_NET   = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}


_BE_CHAIN = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base"}


_MO_EVM   = {"ETH": "eth", "BNB": "bsc", "BASE": "base", "ARC": "arc"}


_SLUG_TO_DISP = {"solana": "SOL", "ethereum": "ETH", "eth": "ETH", "bsc": "BNB",
                 "base": "BASE", "ton": "TON", "robinhood": "RH", "arc": "ARC"}


def _alt_result(chain, symbol, price, mcap, name="", source="", mc_is_fdv=False):
    """Build a dict in the exact shape _parse_dex_pairs() returns.

    mc_is_fdv: True when `mcap` was only obtainable via a fully-diluted-value
    fallback (no real circulating marketCap field was available from this
    provider). FDV is computed off max/total supply, not circulating supply,
    and can read 10-100x+ higher than the token's real market cap for tokens
    with large locked/uncirculated allocations - this is what caused fresh
    listings (e.g. Robinhood-chain tokens) to show as millions in MC when the
    caller's real entry was only thousands. Callers that need entry-MC
    accuracy (e.g. _reconcile_entry_mc) use this flag to avoid trusting an
    FDV-derived number over the KOL's own stated MC.
    """
    try:
        price = float(price or 0)
    except Exception:
        price = 0.0
    try:
        mcap = float(mcap or 0)
    except Exception:
        mcap = 0.0
    if price <= 0 and mcap <= 0 and not symbol:
        return None
    dexpath = CHAIN_TO_DEXPATH.get(chain, "solana") if "CHAIN_TO_DEXPATH" in globals() else "solana"
    return {
        "chain":            chain,
        "mcap":             mcap,
        "mcap_fmt":         fmt_mc(mcap) if mcap > 0 else "N/A",
        "price":            price,
        "symbol":           symbol or "",
        "name":             name or symbol or "",
        "pair_addr":        "",
        "tg_link":          "",
        "price_change_h1":  0.0,
        "price_change_h24": 0.0,
        "volume_h24":       0.0,
        "liquidity_usd":    0.0,
        "txns_buys":        0,
        "txns_sells":       0,
        "listed_at":        None,
        "_partial":         (price <= 0 and mcap <= 0),
        "_source":          source or "alt",
        "_dexpath":         dexpath,
        "_mc_is_fdv":       bool(mc_is_fdv and mcap > 0),
    }


def _alt_chains_for(ca: str) -> list:
    """Display-chain candidates for a contract address."""
    return [_SLUG_TO_DISP.get(s, "SOL") for s in _dex_candidate_chains(ca)
            if s in _SLUG_TO_DISP]


def _src_geckoterminal(ca: str, chains: list):
    for ch in chains:
        net = _GT_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            a = (r.json().get("data") or {}).get("attributes") or {}
            price   = float(a.get("price_usd") or 0)
            mc_real = float(a.get("market_cap_usd") or 0)
            mc_fdv  = float(a.get("fdv_usd") or 0)
            mc      = mc_real or mc_fdv
            res   = _alt_result(ch, a.get("symbol") or "", price, mc,
                                a.get("name") or "", "geckoterminal",
                                mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"GeckoTerminal source failed ({ch}): {e}")
    return None


def _src_dexpaprika(ca: str, chains: list):
    for ch in chains:
        net = _DP_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.dexpaprika.com/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            d  = r.json() or {}
            sm = d.get("summary") or {}
            price   = float(sm.get("price_usd") or d.get("price_usd") or 0)
            mc_real = float(d.get("market_cap_usd") or sm.get("market_cap_usd") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "dexpaprika",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"DexPaprika source failed ({ch}): {e}")
    return None


def _src_birdeye(ca: str, chains: list):
    if not BIRDEYE_API_KEY:
        return None
    for ch in chains:
        bchain = _BE_CHAIN.get(ch)
        if not bchain:
            continue
        if not _birdeye_rate_budget_ok():
            continue  # account-wide budget used up this minute - skip, try next chain/next tick
        try:
            r = _dex_session.get(
                "https://public-api.birdeye.so/defi/token_overview",
                params={"address": ca},
                headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                         "accept": "application/json"},
                timeout=8)
            if r.status_code != 200:
                continue
            d = (r.json() or {}).get("data") or {}
            if not d:
                continue
            price   = float(d.get("price") or 0)
            mc_real = float(d.get("marketCap") or d.get("mc") or d.get("realMc") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "birdeye",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"Birdeye source failed ({ch}): {e}")
    return None


def _src_moralis(ca: str, chains: list):
    if not MORALIS_API_KEY:
        return None
    hdr = {"X-API-Key": MORALIS_API_KEY, "accept": "application/json"}
    for ch in chains:
        try:
            if ch == "SOL":
                r = _dex_session.get(
                    f"https://solana-gateway.moralis.io/token/mainnet/{ca}/price",
                    headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = ""
                mc    = 0.0
                try:
                    rm = _dex_session.get(
                        f"https://solana-gateway.moralis.io/token/mainnet/{ca}/metadata",
                        headers=hdr, timeout=8)
                    if rm.status_code == 200:
                        m   = rm.json() or {}
                        sym = m.get("symbol") or ""
                        supply = float(m.get("supply") or 0)
                        dec    = int(m.get("decimals") or 0) if str(m.get("decimals") or "").isdigit() else 0
                        if supply > 0 and price > 0:
                            if dec and supply > 10 ** 12:
                                supply = supply / (10 ** dec)
                            mc = supply * price
                except Exception:
                    pass
                res = _alt_result("SOL", sym, price, mc, sym, "moralis")
                if res:
                    return res
            else:
                evm = _MO_EVM.get(ch)
                if not evm:
                    continue
                r = _dex_session.get(
                    f"https://deep-index.moralis.io/api/v2.2/erc20/{ca}/price",
                    params={"chain": evm}, headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = (d.get("tokenSymbol") or "")
                res = _alt_result(ch, sym, price, 0, d.get("tokenName") or sym, "moralis")
                if res:
                    return res
        except Exception as e:
            logger.debug(f"Moralis source failed ({ch}): {e}")
    return None


def _src_coinmarketcap(ca: str, chains: list):
    if not CMC_API_KEY:
        return None
    hdr = {"X-CMC_PRO_API_KEY": CMC_API_KEY, "Accept": "application/json"}
    try:
        r = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info",
            params={"address": ca}, headers=hdr, timeout=10)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        if not data:
            return None
        cid, info = next(iter(data.items()))
        if isinstance(info, list):
            info = info[0] if info else {}
        sym  = info.get("symbol") or ""
        name = info.get("name") or ""
        rq = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest",
            params={"id": cid}, headers=hdr, timeout=10)
        price = mc = 0.0
        if rq.status_code == 200:
            q = ((rq.json() or {}).get("data") or {}).get(str(cid)) or {}
            if isinstance(q, list):
                q = q[0] if q else {}
            usd   = (q.get("quote") or {}).get("USD") or {}
            price = float(usd.get("price") or 0)
            mc    = float(usd.get("market_cap") or usd.get("fully_diluted_market_cap") or 0)
        ch = (chains or ["SOL"])[0]
        return _alt_result(ch, sym, price, mc, name, "coinmarketcap")
    except Exception as e:
        logger.debug(f"CoinMarketCap source failed: {e}")
    return None


def _fetch_alt_sources_sync(ca: str):
    """DexScreener ke fail hone par baaki sources try karo (order = fastest first)."""
    if not ca or not EXTRA_SOURCES_ENABLED:
        return None
    chains = _alt_chains_for(ca) or ["SOL"]
    # Birdeye first ONLY for chain-unambiguous addresses (SOL/TON) - it's a
    # paid, independently-indexed source and far more reliable than the free
    # geckoterminal/dexpaprika endpoints (which often return mc=N/A for
    # fresh/low-cap tokens). For EVM (0x...) addresses the chain itself is a
    # guess across ETH/BSC/Base/Robinhood, and Birdeye just takes the first
    # chain with any data - which can be a wrong/unrelated token on a
    # different chain with a very different MC. So for those, try the
    # chain-neutral sources first and treat Birdeye as a last resort there.
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    order = ((_src_geckoterminal, _src_dexpaprika, _src_birdeye,
              _src_moralis, _src_coinmarketcap) if is_evm_ambiguous else
             (_src_birdeye, _src_geckoterminal, _src_dexpaprika,
              _src_moralis, _src_coinmarketcap))
    # SPEED_NOV12: all alt sources start at once (was: strictly sequential,
    # so a slow first source blocked every later one - up to 5 x 8s = 40s).
    # Results are still taken in the same priority order: the first source
    # (by priority) with data wins, exactly as before. Only the wall time
    # changes, capped by a shared 10s deadline. Stragglers are left to
    # finish in the background (shutdown(wait=False)) so an early hit is
    # never delayed by a slow lower-priority source.
    try:
        import concurrent.futures as _cf
        _deadline = time.time() + 10
        _ex = _cf.ThreadPoolExecutor(max_workers=len(order))
        try:
            _futs = [_ex.submit(fn, ca, chains) for fn in order]
            for _fn, _f in zip(order, _futs):
                try:
                    res = _f.result(timeout=max(0.1, _deadline - time.time()))
                except Exception as e:
                    logger.debug(f"alt source {getattr(_fn, '__name__', '?')} error: {e}")
                    res = None
                if res and (res.get("price", 0) > 0 or res.get("mcap", 0) > 0 or res.get("symbol")):
                    logger.info(f"🛰️ Fallback source {res.get('_source')} hit for {ca[:12]}… "
                                f"({res.get('symbol','?')} mc={res.get('mcap_fmt','N/A')})")
                    return res
        finally:
            _ex.shutdown(wait=False)
    except Exception as e:
        logger.debug(f"alt sources parallel run failed: {e}")
    return None


_BIRDEYE_BULK_429_UNTIL = 0.0


_BIRDEYE_BULK_COOLDOWN  = int(os.environ.get("BIRDEYE_BULK_429_COOLDOWN_SECONDS", "15") or 15)


def _bulk_refresh_birdeye_sync(cas: list, chain_map: dict) -> int:
    """DexScreener breaker open hone par bulk batch ko Birdeye multi_price se
    refresh karta hai - /defi/multi_price ek request mein 100 tak addresses
    accept karta hai. chain_map: {ca: 'SOL'|'ETH'|'BNB'|'BASE', ...} - jo
    tracked_calls['chain'] se already pata hai, guess nahi karna padta.
    NOTE: this account's key is the FREE tier (60 req/min total, shared with
    _src_birdeye's single-CA calls) - see _birdeye_rate_budget_ok(), which
    every sub-batch request below checks first so this fallback can't blow
    through the account's own limit and start 429-ing itself.
    """
    import time as _time
    if not BIRDEYE_API_KEY or not cas:
        return 0
    global _BIRDEYE_BULK_429_UNTIL
    if _time.time() < _BIRDEYE_BULK_429_UNTIL:
        return 0
    by_chain: dict = {}
    for ca in cas:
        bchain = _BE_CHAIN.get((chain_map or {}).get(ca, "SOL"), "solana")
        by_chain.setdefault(bchain, []).append(ca)
    updated = 0
    for bchain, addrs in by_chain.items():
        for i in range(0, len(addrs), 100):
            sub = addrs[i:i + 100]
            if not _birdeye_rate_budget_ok():
                logger.debug("Birdeye bulk refresh: account budget used up this minute, skipping rest")
                return updated
            try:
                r = _dex_session.get(
                    "https://public-api.birdeye.so/defi/multi_price",
                    params={"list_address": ",".join(sub)},
                    headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                             "accept": "application/json"},
                    timeout=8)
            except Exception as e:
                logger.debug(f"Birdeye bulk refresh network error: {e}")
                continue
            if r.status_code == 429:
                _BIRDEYE_BULK_429_UNTIL = _time.time() + _BIRDEYE_BULK_COOLDOWN
                logger.warning("Birdeye bulk refresh 429 - cooling down, DexScreener single-CA fallback will cover this tick")
                return updated
            if r.status_code != 200:
                continue
            data = (r.json() or {}).get("data") or {}
            disp_chain = _SLUG_TO_DISP.get(bchain, (chain_map or {}).get(sub[0], "SOL"))
            for addr, d in data.items():
                if not d:
                    continue
                price = float(d.get("value") or d.get("price") or 0)
                if price <= 0:
                    continue
                result = _alt_result(disp_chain, d.get("symbol") or "", price,
                                      float(d.get("marketCap") or d.get("mc") or 0),
                                      d.get("name") or "", "birdeye_bulk")
                if result:
                    # Actual tracked chain (not the display slug) so the caller's
                    # symbol/chain assignment on the call stays correct.
                    result["chain"] = (chain_map or {}).get(addr, disp_chain)
                    _dex_cache[addr] = (result, _time.time() + _DEX_CACHE_TTL)
                    _dex_miss_cache.pop(addr, None)
                    updated += 1
    if updated:
        logger.info(f"🐦 Birdeye bulk refresh filled {updated} token(s) while DexScreener breaker was open")
    return updated


def _bulk_refresh_dex_sync(cas: list, min_liquidity: int = 0, chain_map: dict = None) -> int:
    """Refresh the DexScreener cache for many tokens with few requests.

    /latest/dex/tokens/{a,b,c} accepts up to 30 comma-separated addresses, so
    tracking 300 tokens costs ~10 requests instead of 300. This is what makes
    real-time (5 s) tracking possible without hitting rate limits.
    """
    import time as _time
    uniq, seen_local = [], set()
    for ca in cas or []:
        if ca and ca not in seen_local:
            seen_local.add(ca)
            uniq.append(ca)
    updated = 0
    for i in range(0, len(uniq), 30):
        batch = uniq[i:i + 30]
        data = _dex_get(f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}", fresh=True, lane="bulk")
        if data == 429:
            # Breaker open ho chuka hai - baaki batches bhi 429 hi hongi.
            # Is batch aur baaki sab ko Birdeye se cover karo (paisay already
            # diye hue hain isi ke liye) taake tokens poori tarah stale na
            # rahein jab tak DexScreener cooldown khatam nahi hota.
            remaining = uniq[i:]
            try:
                updated += _bulk_refresh_birdeye_sync(remaining, chain_map or {})
            except Exception as e_be:
                logger.warning(f"Birdeye bulk fallback failed: {e_be}")
            _now_w = _time.time()
            if _now_w - globals().get("_BULK429_LOG_AT", 0.0) >= 10:
                globals()["_BULK429_LOG_AT"] = _now_w
                logger.warning("DexScreener 429 bulk refresh - is tick ke baaki batches skip "
                               "(breaker open / budget khatam; details: '📊 DexScreener last min' line)")
            break
        pairs = (data or {}).get("pairs") or []
        if not pairs:
            continue
        grouped: dict = {}
        wanted = {c.lower(): c for c in batch}
        for p in pairs:
            # DexScreener's priceUsd belongs to baseToken only. Never assign a
            # quote-token pair to a tracked CA (that tracks SOL/WETH by mistake).
            addr = ((p.get("baseToken") or {}).get("address") or "").lower()
            if addr in wanted:
                grouped.setdefault(wanted[addr], []).append(p)
        for ca, plist in grouped.items():
            result = _parse_dex_pairs(plist, min_liquidity, ca)
            if result:
                _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                updated += 1
        if i + 30 < len(uniq):
            _time.sleep(0.08)
    _dex_cache_gc()
    return updated


def _get_cached_dex(ca: str):
    """Return only the current bulk-refresh result; never perform network I/O.

    The monitoring loop used to call _fetch_dex_sync once per tracked token
    after it had already done a bulk request.  On a cache miss that spawned
    dozens of slow requests (and fallback-provider requests) in the same tick,
    causing the next tick to overlap and making X alerts arrive late.
    """
    if not ca:
        return None
    cached = _dex_cache.get(ca)
    if not cached:
        return None
    result, expiry = cached
    if time.time() >= expiry:
        return None
    return result


DIP_MILESTONES_BASE = [2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 35, 40, 45, 50,
                       60, 70, 80, 90, 100, 150, 200, 250, 500, 1000, 1500]


DIP_MAX_X = 10000


DIP_DEFAULT_THRESHOLD_PCT = 30.0


DIP_DUST_FLOOR_FRAC = 0.01


DIP_LOW_CONFIRM_TICKS = 1


DIP_RESET_BELOW_FRAC = 0.8


def _dip_cfg():
    try:
        return load_config()
    except Exception:
        return {}


def dip_enabled():
    try:
        return bool(_dip_cfg().get("dip_enabled", True))
    except Exception:
        return True


def dip_threshold_pct():
    try:
        v = float(_dip_cfg().get("dip_threshold_pct", DIP_DEFAULT_THRESHOLD_PCT)
                  or DIP_DEFAULT_THRESHOLD_PCT)
    except (TypeError, ValueError):
        v = DIP_DEFAULT_THRESHOLD_PCT
    return min(max(v, 1.0), 50.0)


def dip_milestones_for_ratio(ratio):
    try:
        ratio = float(ratio or 0)
    except (TypeError, ValueError):
        return []
    out = [m for m in DIP_MILESTONES_BASE if ratio >= m]
    m = 2000
    while m <= ratio and m <= DIP_MAX_X:
        out.append(m)
        m += 500
    return out


def _dip_sent_levels(call):
    v = call.get("sent_dip_milestones")
    if not isinstance(v, list):
        v = []
        call["sent_dip_milestones"] = v
    return v


def call_peak_ratio(call):
    """Best known ATH multiplier for a tracked call (never below live ratio)."""
    try:
        pr = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        pr = 0.0
    try:
        lr = float(call.get("last_ratio", 0) or 0)
    except (TypeError, ValueError):
        lr = 0.0
    return max(pr, lr)


def _verified_live_ratio_inner(call, dex):
    """Return a market-cap-first live ratio, or 0 when the sample is unsafe.

    Price ratios are not interchangeable across pool migrations, decimal/supply
    changes or bad pair responses. Market cap is therefore authoritative. A
    price ratio may only be used when MC is unavailable, and conflicting price
    data can never inflate a valid MC ratio - UNLESS the MC field itself looks
    stale/frozen (see below), in which case it's not really "data" anymore.
    """
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
        entry_price = float(call.get("entry_price", 0) or 0)
        cur_mc = float((dex or {}).get("mcap", 0) or 0)
        cur_price = float((dex or {}).get("price", 0) or 0)
    except (TypeError, ValueError):
        return 0.0

    # OWNER FIX (2026-09-19, PDOG + repeat incidents): GeckoTerminal /
    # DexPaprika / Birdeye fall back to FDV (fully-diluted valuation) when a
    # token has no real circulating-supply market_cap field. FDV counts
    # locked/team/treasury supply too, so it can read 3-10x+ higher than the
    # actual circulating MC shown on-chart - a single FDV-tagged tick was
    # enough to fire fake 4X/30X/40X alerts on tokens that hadn't moved.
    # This was previously only guarded at entry-MC time (_reconcile_entry_mc,
    # live_is_fdv=...); ongoing live-ratio checks had no such guard. Since
    # every X-alert/milestone/record path ultimately calls this one function
    # for its ratio, checking `_mc_is_fdv` here protects all of them at once
    # - an FDV-derived MC is treated as "no MC data this tick" (falls back to
    # price-ratio, or 0.0 / no alert) instead of ever being trusted directly.
    if (dex or {}).get("_mc_is_fdv"):
        cur_mc = 0.0

    mc_ratio = cur_mc / entry_mc if entry_mc > 0 and cur_mc > 0 else 0.0
    price_ratio = cur_price / entry_price if entry_price > 0 and cur_price > 0 else 0.0
    if mc_ratio > 0:
        if price_ratio > 0:
            # FIX (rug-pull supply-manipulation bug, SCMP/@robinhoodcallsbot
            # incident, 2026-09-20): price_ratio and mc_ratio come from the
            # SAME DexScreener response but can be computed from different
            # fields (raw price vs the reported marketCap/totalSupply). A
            # legitimate supply change (burn/mint) can only make mc_ratio
            # diverge from price_ratio in magnitude - it can NEVER flip which
            # DIRECTION the token moved, because MC is mathematically
            # price × supply: if price is really down, MC cannot be really up
            # unless supply itself exploded, which is exactly what a rug
            # contract does to its own reported supply. Here price_ratio was
            # 0.61x (correctly showing the token had crashed - real MC ~2K
            # against an entry of 4.8K) while mc_ratio claimed 39.73x - the
            # old code trusted mc_ratio purely because it wasn't "frozen at
            # exactly 1.00x", and fired 16X through 35X fake milestone alerts
            # on a dead token. A direction conflict (one side up, one side
            # down) is a much stronger and simpler red flag than the narrow
            # frozen-at-1.0 case below, and price is far harder to fake than a
            # reported supply/MC field, so it wins outright whenever they
            # disagree on direction - no divergence threshold needed.
            if (price_ratio < 0.97 and mc_ratio > 1.03) or (price_ratio > 1.03 and mc_ratio < 0.97):
                logger.warning(
                    f"🚫 Direction conflict: price ratio {price_ratio:.2f}x vs "
                    f"DexScreener MC ratio {mc_ratio:.2f}x for "
                    f"{call.get('ca','')[:12]}… - MC/supply field is almost "
                    f"certainly manipulated or stale (likely a rug); trusting "
                    f"price, which cannot be faked the same way.")
                return price_ratio
            divergence = max(mc_ratio, price_ratio) / max(min(mc_ratio, price_ratio), 1e-12)
            if divergence > 3:
                # OWNER FIX (2026-09-14): DexScreener's cached `marketCap`
                # field can freeze for hours at a time while `priceUsd` keeps
                # updating normally - a stale field on their side, not the
                # token actually standing still. This is only reliably
                # detectable in one specific case: MC frozen almost exactly
                # AT the entry value (mc_ratio ~1.00x) for a call that has
                # clearly moved - a real, actively-traded token essentially
                # never sits at exactly 1.000x for many ticks in a row, so
                # that coincidence is a strong, narrow signal the field
                # itself is stuck (proven correct for the DOGGO/minibangcall
                # incident: entry pinned every X at 1x forever).
                #
                # REVERTED (2026-09-14, same day, IDX/casinoinsiderhere
                # incident): a broader "MC unchanged for N ticks at ANY
                # level → treat as stale" version briefly replaced this and
                # made things worse - it fired on this token's mc_ratio
                # sitting normally at ~1.33-1.41x (a real, legitimately flat
                # stretch, not stale data) while price_ratio bounced around
                # 0.28-0.30x from ordinary quote noise, and confidently
                # trusted the WRONG noisy price over the CORRECT flat MC.
                # A token's market cap simply not moving much for a while is
                # completely normal and is not by itself evidence of a stuck
                # field - only the exact-1.00x coincidence is. Narrowed back.
                mc_frozen_at_entry = abs(mc_ratio - 1.0) < 0.03
                if mc_frozen_at_entry:
                    # BUG FIX (2026-09-14, live incident - channel called a ~1T MC
                    # token, entry got captured in the "millions" instead, then the
                    # bot spammed 2X/3X/4X/... alerts back to back): trusting price
                    # unconditionally whenever MC looks frozen was too permissive.
                    # A real pump ratchets up gradually tick by tick; a jump straight
                    # from ~1x to thousands-of-x in one sample (5575x was observed
                    # live) is virtually always bad data - wrong decimals, a
                    # rebased/forked pair, or a bad quote - not a real move. Trusting
                    # it meant every milestone from 2X up to 1000X+ fired at once in
                    # a single tick: an alert-spam burst, not thousands of real X.
                    # Cap it: an implausible jump is rejected and held at the call's
                    # last confirmed peak instead of being allowed through.
                    _prev_peak = 0.0
                    try:
                        _prev_peak = float(call.get("peak_ratio", 0) or 0)
                    except (TypeError, ValueError):
                        _prev_peak = 0.0
                    _implausible = price_ratio > 1000 or (
                        _prev_peak > 50 and price_ratio > _prev_peak * 50)
                    if _implausible:
                        logger.warning(
                            f"Rejecting implausible price ratio {price_ratio:.2f}x "
                            f"(prev peak {_prev_peak:.2f}x, MC ratio {mc_ratio:.2f}x) "
                            f"for {call.get('ca','')[:12]}… - likely bad decimals/pair "
                            f"data, not a real move; holding at last-known ratio")
                        return _prev_peak if _prev_peak > 0 else mc_ratio
                    logger.warning(
                        f"MC ratio frozen at entry ({mc_ratio:.2f}x) but price ratio is "
                        f"{price_ratio:.2f}x for {call.get('ca','')[:12]}… - trusting price instead")
                    return price_ratio
                # FIX (2026-09-30, BABYBUN/@spawnscalls fake 30X): MC = price x
                # supply, so an MC ratio that claims a big pump (>=2x) while
                # PRICE says it moved >3x less is the supply/MC field being
                # wrong or stale, not a real 26x. A pump claim must be backed by
                # price too - take the lower of the two. (Ratios < 2x are left
                # alone so the IDX/DOGGO fixes above keep behaving as before.)
                if MC_PRICE_PUMP_GUARD and mc_ratio >= 2 and mc_ratio > price_ratio:
                    logger.warning(
                        f"🛡️ MC-vs-price pump guard: MC ratio {mc_ratio:.2f}x but price "
                        f"ratio only {price_ratio:.2f}x for {call.get('ca','')[:12]}… "
                        f"- using price ratio")
                    return price_ratio
                logger.warning(
                    f"Ignoring conflicting price ratio {price_ratio:.2f}x; "
                    f"DexScreener MC ratio is {mc_ratio:.2f}x for {call.get('ca','')[:12]}…")
        return mc_ratio
    # Last-resort price tracking is deliberately conservative. Once a real MC
    # appears, all future ticks automatically switch to MC-first tracking.
    if 0 < price_ratio <= 1000:
        return price_ratio
    return 0.0


def _verified_live_ratio(call, dex):
    """Outer safety wrapper around _verified_live_ratio_inner().

    Every specific bad-data pattern found so far (frozen-at-1.0x MC field,
    price/MC direction conflicts, FDV-tagged MC, implausible one-tick jumps
    while frozen) has its own targeted fix inside the inner function. Each
    one only catches the exact pattern of the incident that revealed it -
    which is why this exact class of bug (a rugged/dead token being handed a
    wildly wrong MC from SOME source, in SOME shape nobody has hit yet) kept
    recurring under new disguises. This wrapper is the backstop: regardless
    of which internal path produced a number, or whether it's a shape this
    file has ever seen before, a jump this large in a single tick relative to
    the call's own last CONFIRMED peak is rejected outright and the peak is
    held instead - a real pump does not advance 20x+ beyond its own prior
    high between two ~2-second monitoring ticks. This does not replace the
    inner checks (they still produce a better-reasoned number/log message for
    the common cases); it only adds an absolute ceiling nothing can slip
    past, including patterns this file doesn't have a name for yet.
    """
    ratio = _verified_live_ratio_inner(call, dex)
    if ratio <= 0:
        return ratio
    # FIX (2026-10-18, $DACITY fake 2X..13X burst after a redeploy): a quote that came from an
    # ALTERNATE source (GeckoTerminal/Birdeye/...) -- e.g. while DexScreener is rate-limited or
    # right after a restart -- can be frozen/stale (Gecko kept returning 44.4K all day while
    # DexScreener showed 3.3K). The old code let such a quote produce ANY ratio up to 20x/1000x
    # when the call had no established peak yet. An alt-source quote may now never imply more
    # than the same ceiling the "unstick" probe uses (max(8x, +5x) over the last peak);
    # DexScreener's own quotes are unaffected.
    _qsrc = str((dex or {}).get("_source") or "").lower()
    if _qsrc and _qsrc != "dexscreener" and ALT_SOURCE_STRICT:
        # FIX (2026-10-21, "200k peak -> 40k par call -> free X"): token pehle 200k tak gaya, 40k par aa gaya,
        # caller ne 40k par call ki. Flat (~1.0x) tokens par alt-source probe chalta hai; Gecko/Birdeye
        # ka purana 200k (stale peak) 5x bana kar nikalta tha aur purana 8x ceiling usay pass kar deta tha.
        # Ab alt source akele X nahi bana sakta: ratio <= max(ALT_FREE_RATIO, DexScreener ratio * ALT_MAX_LEAD).
        # DexScreener ka apna reading na ho (ya purana ho) to sirf ALT_FREE_RATIO (1.9x, 2X se neeche) tak.
        try:
            _dx_r = float(call.get("_dx_ratio", 0) or 0)
            _dx_t = float(call.get("_dx_at", 0) or 0)
        except (TypeError, ValueError):
            _dx_r, _dx_t = 0.0, 0.0
        _fresh_dx = _dx_r > 0 and (time.time() - _dx_t) <= ALT_DX_MAX_AGE_S
        _alt_ceiling = max(ALT_FREE_RATIO, _dx_r * ALT_MAX_LEAD) if _fresh_dx else ALT_FREE_RATIO
        if ratio > _alt_ceiling:
            logger.warning(
                f"🚫 Alt-source ratio {ratio:.2f}x ({_qsrc}) rejected for {call.get('ca','')[:12]}… - "
                f"DexScreener-confirmed ratio {_dx_r:.2f}x (alt ceiling {_alt_ceiling:.2f}x). "
                f"Alt source akele X nahi bana sakta (stale-peak guard).")
            return min(call_peak_ratio(call), _alt_ceiling) if call_peak_ratio(call) > 0 else 0.0
    if _qsrc and _qsrc != "dexscreener":
        _base = max(call_peak_ratio(call), 1.0)
        if ratio > max(_base * 8, _base + 5):
            logger.warning(
                f"🚫 Rejecting alt-source ratio {ratio:.2f}x ({_qsrc}) for "
                f"{call.get('ca','')[:12]}… - too far above last peak {_base:.2f}x for an "
                f"unverified source; waiting for DexScreener to confirm.")
            return call_peak_ratio(call)
    prev_peak = call_peak_ratio(call)
    if prev_peak <= 0:
        # First-ever real reading for this call - no established baseline to
        # compare against yet, so only guard against obviously-broken data
        # (wrong decimals etc.), same ceiling already used for that case.
        if ratio > 1000:
            logger.warning(
                f"🚫 Rejecting implausible first-ever ratio {ratio:.2f}x for "
                f"{call.get('ca','')[:12]}… (no established peak yet) - "
                f"holding at 0 (no alert this tick).")
            return 0.0
        return ratio
    if ratio > max(prev_peak * 20, 50):
        logger.warning(
            f"🚫 Rejecting implausible jump {prev_peak:.2f}x → {ratio:.2f}x "
            f"for {call.get('ca','')[:12]}… (>20x prior peak in one tick) - "
            f"holding at last confirmed peak {prev_peak:.2f}x instead.")
        return prev_peak
    return ratio


ALT_SOURCE_STRICT       = (os.environ.get("ALT_SOURCE_STRICT", "1") or "1").strip().lower() in ("1", "true", "yes")


ALT_FREE_RATIO          = float(os.environ.get("ALT_FREE_RATIO", "1.9") or 1.9)


ALT_MAX_LEAD            = float(os.environ.get("ALT_MAX_LEAD", "1.25") or 1.25)


ALT_DX_MAX_AGE_S        = float(os.environ.get("ALT_DX_MAX_AGE_SECONDS", "900") or 900)


_dip = {}


_queue = {}


_queued_set = set()


_dip_dirty = False


_dip_last_save = 0.0


def _queue_key(call_key, x_val, typ):
    return f"{call_key}:{x_val}:{typ}"


def _age_seconds(call):
    try:
        return (datetime.utcnow() - datetime.fromisoformat(
            str(call.get("tracked_since", "")))).total_seconds()
    except Exception:
        return 999999.0


def _apply_dip_to_call(call_key, call):
    st = _dip.get(call_key)
    if not st:
        return
    for field in ("dip_low", "_dip_low_candidate", "_dip_low_candidate_ticks",
                  "dip_low_at_alert", "sent_dip_milestones", "pending_dip",
                  "dip_pending_mc"):
        if field in st:
            call[field] = copy.deepcopy(st[field])


def _sync_call_to_dip(call_key, call):
    global _dip_dirty
    st = _dip.setdefault(call_key, {})
    changed = False
    for field in ("dip_low", "_dip_low_candidate", "_dip_low_candidate_ticks",
                  "dip_low_at_alert", "sent_dip_milestones", "pending_dip",
                  "dip_pending_mc"):
        v = copy.deepcopy(call.get(field))
        if v != st.get(field):
            st[field] = v
            changed = True
    if changed:
        _dip_dirty = True
    return changed


def _maybe_save_dip(force=False):
    global _dip_dirty, _dip_last_save
    now = time.time()
    if not _dip_dirty:
        return
    if not force and now - _dip_last_save < 60:
        return
    try:
        _atomic_write_json(DIPBOT_DIP_FILE, _dip)
        _dip_last_save = now
        _dip_dirty = False
    except Exception as e:
        logger.warning(f"dipbot: dip save failed: {e}")


def _queue_entry(call_key, call, x_val, typ, peak_ratio, live_ratio, cur_mc,
                 ms_mc_fmt, record_only=False, dip_low=0.0):
    key = _queue_key(call_key, x_val, typ)
    if key in _queue:
        return False
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
    except (TypeError, ValueError):
        entry_mc = 0.0
    entry = {
        "type": typ,
        "call_key": call_key,
        "channel": call.get("channel", ""),
        "ca": call.get("ca", ""),
        "symbol": call.get("symbol", ""),
        "x_val": int(x_val),
        "peak_mc": float(call.get("peak_mc", 0) or 0),
        "ms_mc_fmt": ms_mc_fmt,
        "entry_mc": entry_mc,
        "entry_fmt": call.get("entry_fmt", "N/A"),
        "live_ratio": round(float(live_ratio or 0), 4),
        "detected_at": datetime.utcnow().isoformat(),
        "status": "queued",
        "record_only": bool(record_only),
    }
    if typ == "dip":
        entry["dip_low"] = float(dip_low or 0)
        entry["cur_mc"] = float(cur_mc or 0)
    _queue[key] = entry
    _queued_set.add((call_key, int(x_val), typ))
    try:
        _atomic_write_json(DIPBOT_QUEUE_FILE, _queue)
    except Exception as e:
        logger.warning(f"dipbot: queue save failed: {e}")
    logger.info(
        f"dipbot: queued {x_val}X {call.get('symbol', '?')} "
        f"@{call.get('channel', '?')} peak={peak_ratio:.2f} live={live_ratio:.2f}")
    return True


async def _dipbot_dip_tick(call_key, call, cur_mc):
    if not dip_enabled():
        return
    try:
        entry_mc = float(call.get("entry_mc") or 0)
        cur_mc = float(cur_mc or 0)
    except (TypeError, ValueError):
        return
    if entry_mc <= 0 or cur_mc <= 0:
        return
    threshold = dip_threshold_pct()
    dip_low = call.get("dip_low")
    try:
        dip_low = float(dip_low) if dip_low else 0.0
    except (TypeError, ValueError):
        dip_low = 0.0
    if cur_mc <= entry_mc * (1.0 - threshold / 100.0):
        if cur_mc >= max(1000.0, entry_mc * DIP_DUST_FLOOR_FRAC):  # DIPBOT: DIPFLOOR1K - fixed 1K MC floor
            cand = call.get("_dip_low_candidate")
            try:
                cand = float(cand) if cand else 0.0
            except (TypeError, ValueError):
                cand = 0.0
            if cand <= 0 or cur_mc < cand:
                call["_dip_low_candidate"] = cur_mc
                call["_dip_low_candidate_ticks"] = 1
            elif cur_mc <= cand * 1.05:
                call["_dip_low_candidate_ticks"] = int(call.get("_dip_low_candidate_ticks") or 0) + 1
            else:
                call["_dip_low_candidate"] = cur_mc
                call["_dip_low_candidate_ticks"] = 1
            if int(call.get("_dip_low_candidate_ticks") or 0) >= DIP_LOW_CONFIRM_TICKS:
                cand = float(call.get("_dip_low_candidate") or 0)
                if cand > 0:
                    dip_low = cand if dip_low <= 0 else min(dip_low, cand)
                    call["dip_low"] = dip_low
                    try:
                        cycle_low = float(call.get("dip_low_at_alert") or 0)
                    except (TypeError, ValueError):
                        cycle_low = 0.0
                    if cycle_low <= 0:
                        call["dip_low_at_alert"] = cand
                    elif cand <= cycle_low * DIP_RESET_BELOW_FRAC:
                        call["sent_dip_milestones"] = []
                        call["pending_dip"] = []
                        call.pop("dip_pending_mc", None)
                        call["dip_low_at_alert"] = cand
                        logger.info(f"Dip cycle reset {call_key}: new low "
                                    f"{fmt_mc(cand)} is 20%+ below cycle low "
                                    f"{fmt_mc(cycle_low)} - sent dip milestones cleared")
                call.pop("_dip_low_candidate", None)
                call.pop("_dip_low_candidate_ticks", None)
    if dip_low <= 0:
        _sync_call_to_dip(call_key, call)
        return
    dip_ratio = cur_mc / dip_low
    if dip_ratio < 2:
        _sync_call_to_dip(call_key, call)
        return
    sent = _dip_sent_levels(call)
    # Never re-queue a level the main bot already sealed in its own record.
    main_sent = call.get("sent_dip_milestones") or []
    due = [m for m in dip_milestones_for_ratio(dip_ratio)
           if m not in sent and m not in main_sent]
    if not due:
        _sync_call_to_dip(call_key, call)
        return
    due.sort()
    for ms in due:
        if (call_key, int(ms), "dip") in _queued_set:
            continue
        # DIPBOT: never queue a level with no media set - park it in
        # dipbot state (pending_dip) and retry every 15s. The main bot
        # must never get a media-less dip post (media hold rule: NEVER
        # text-only). Parked levels are NOT marked sent, so the tick
        # re-fires them the moment media is set.
        if not dip_media_for(ms):
            _dip_park_level(call, ms, cur_mc)
            continue
        ms_mc_fmt = fmt_mc(entry_mc * ms) if entry_mc > 0 else fmt_mc(cur_mc)
        if _queue_entry(call_key, call, ms, "dip", dip_ratio, dip_ratio,
                        cur_mc, ms_mc_fmt, dip_low=dip_low):
            # DIPBOT: queued, not sent. sent_dip_milestones stays the
            # main bot's sealed list; _queued_set tracks this level
            # in flight. Unpark it if it was parked for no media.
            _dip_unpark_level(call, ms)
    _sync_call_to_dip(call_key, call)


# === DIPBOT-SPECIFIC (not extracted from monbot.py) ==========================
# The extracted code above ends with the dip tick + queue + price stack.
# Everything below is dipbot's own: its reload/check/loop, the media gate
# helpers, and the entry point. The main bot's sealed list is the source of
# truth for "posted" - see the header.

DIP_DEFAULT_THRESHOLD_PCT = 50.0  # DIPBOT: live default is 50 (DIP5099); stored config governs anyway
MAIN_TRACKED_FILE = os.path.join(DIPBOT_MAIN_DATA_DIR, "tracked_calls.json")
MAIN_MILESTONES_FILE = os.path.join(DIPBOT_MAIN_DATA_DIR, "sent_milestones.json")
_calls = {}
_sent = {}
_mtimes = {"tracked": 0.0, "sent": 0.0}
_DIPBOT_TICK_N = 0
_DIPBOT_MAX_CHECKS_PER_TICK = int(os.environ.get("DIPBOT_MAX_CHECKS_PER_TICK", "110") or 110)
_DIPBOT_RETRY_SECONDS = 15
# Cadence constants (same defaults as monbot's env example).
HOT_WINDOW_SECONDS = int(os.environ.get("HOT_WINDOW_SECONDS", "180") or 180)
WARM_EVERY_N_TICKS = int(os.environ.get("WARM_EVERY_N_TICKS", "2") or 2)
DIPBOT_TICK_SECONDS = float(os.environ.get("DIPBOT_TICK_SECONDS", "2") or 2)
_QUEUE_MAX_AGE_S = 24 * 3600  # queued dip entries older than this are dropped


def dip_media_for(x_val):
    """Media configured for a dip X level (from bot_config.json dip_media)."""
    try:
        m = (load_config().get("dip_media", {}) or {}).get(str(int(x_val)))
    except Exception:
        m = None
    if isinstance(m, dict) and m.get("file_id"):
        return m
    return None


def dip_title_for_x(x_val):
    """Dip title EXACTLY as set by the owner. Dipbot only logs it per queued
    level; the main bot builds the caption at post time (send_dip_alert)."""
    try:
        x = int(x_val)
    except (TypeError, ValueError):
        x = 0
    if 2 <= x <= 9:
        return f"DIP TO PROFIT: {x}X"
    if 10 <= x <= 15:
        return f"DIP SNIPER STRIKES | {x}X"
    if x == 20:
        return "DIP DEMOLISHED 20x Profit"
    if x in (25, 30):
        return f"DIP HUNTERS WIN {x}X"
    if 31 <= x <= 40:
        return f"DIP ENTRY HITS {x}X"
    if x == 45:
        return "DIP MASTERED, 45X DONE"
    if x == 50:
        return "DIP TURNED GOLD, 50x Profit"
    if x == 60:
        return "RIDING THE DIP TO 60X"
    if x == 70:
        return "DIP GAME STRONG, 70X IN"
    if x == 80:
        return "DIP SNIPER PAYS 80X"
    if x == 90:
        return "DIP BUYERS EAT, 90X SERVED"
    if x == 100:
        return "HISTORIC DIP PROFIT 100X"
    if x == 200:
        return "DIP CAUGHT, 200X UNLOCKED"
    if x in (300, 400):
        return f"DIP BOUGHT, LIFE CHANGED {x}X"
    if x == 500:
        return "DIP TURNED LEGENDARY, 500X"
    if 600 <= x <= 1000:
        return f"DIP TO THE MOON, {x}X"
    return f"DIP TO PROFIT: {x}X"


def _dip_park_level(call, x_val, cur_mc=0):
    """Park a due dip level in dipbot state (no media set for it yet).

    NOT marked sent, so the tick re-fires it the moment media is set.
    The 15s retry sweep re-checks parked calls explicitly.
    """
    lst = call.get("pending_dip")
    if not isinstance(lst, list):
        lst = []
        call["pending_dip"] = lst
    if int(x_val) not in lst:
        lst.append(int(x_val))
    try:
        mc_map = call.get("dip_pending_mc")
        if not isinstance(mc_map, dict):
            mc_map = {}
            call["dip_pending_mc"] = mc_map
        mc_map[str(int(x_val))] = float(cur_mc or 0)
    except (TypeError, ValueError):
        pass


def _dip_unpark_level(call, x_val):
    """Remove a level from the parked list (it just got queued)."""
    try:
        lst = call.get("pending_dip")
        if isinstance(lst, list) and int(x_val) in lst:
            lst.remove(int(x_val))
        mc_map = call.get("dip_pending_mc")
        if isinstance(mc_map, dict):
            mc_map.pop(str(int(x_val)), None)
    except (TypeError, ValueError):
        pass


def _dipbot_load_own():
    """Load dipbot's own files (dip state + queue) into memory."""
    global _dip, _queue, _queued_set
    _dip = _read_json_file(DIPBOT_DIP_FILE, {}) or {}
    _queue = _read_json_file(DIPBOT_QUEUE_FILE, {}) or {}
    _queued_set = set()
    for key, e in _queue.items():
        try:
            _queued_set.add((e.get("call_key", ""), int(e.get("x_val", 0)), e.get("type", "x")))
        except (TypeError, ValueError):
            pass


def _dipbot_reload():
    """Reload tracked_calls.json (READ-ONLY) when mtime changes. Dip-only:
    deep-copy each call and apply dipbot's dip state onto it - EXCEPT
    sent_dip_milestones, which is restored from the main bot file value.
    The main bot's sealed list is the source of truth for 'posted' (see
    header). Returns True if the call set changed.
    """
    global _calls
    changed = False
    try:
        mt = os.path.getmtime(MAIN_TRACKED_FILE)
    except OSError:
        mt = 0.0
    if mt != _mtimes["tracked"]:
        raw = _read_json_file(MAIN_TRACKED_FILE, {}) or {}
        if isinstance(raw, dict) and raw:
            new_calls = {}
            for k, v in raw.items():
                if not isinstance(v, dict):
                    continue
                c = copy.deepcopy(v)
                _apply_dip_to_call(k, c)
                # Restore the main bot's sealed list (source of truth).
                _fs = v.get("sent_dip_milestones")
                c["sent_dip_milestones"] = list(_fs) if isinstance(_fs, list) else []
                new_calls[k] = c
            _calls = new_calls
            _mtimes["tracked"] = mt
            changed = True
    return changed


def _dipbot_prune_queue():
    """Drop queued dip entries that are sealed, orphaned, stale, or >24h old.

    Stale = a 'queued' entry older than 10 minutes that the main bot never
    sealed. That means the post failed and the drain unsealed it (or the
    main bot is down). Dropping it lets the tick re-fire the level if the
    live ratio still holds - otherwise a failed post would sit in the queue
    forever and the level could never alert again. The main bot's seal-first
    dedup prevents a double post if the old entry was in fact already
    consumed. Dipbot's queue is dip-only (type is always 'dip').
    """
    global _queue, _queued_set
    now = datetime.utcnow()
    drop = []
    for key, e in list(_queue.items()):
        ck = e.get("call_key", "")
        xv = e.get("x_val", 0)
        try:
            age = (now - datetime.fromisoformat(str(e.get("detected_at", "")))).total_seconds()
        except Exception:
            age = 0.0
        if age > _QUEUE_MAX_AGE_S:
            drop.append(key)
            continue
        if ck not in _calls:
            drop.append(key)
            continue
        call = _calls.get(ck) or {}
        sent_dip = call.get("sent_dip_milestones") or []
        try:
            sealed = int(xv) in [int(x) for x in sent_dip]
        except (TypeError, ValueError):
            sealed = False
        if sealed:
            drop.append(key)
            continue
        if age > 600:
            # Stale queued entry: main bot never sealed it in 10 minutes.
            # Drop it so the tick can re-fire the level if still due.
            drop.append(key)
            continue
    if not drop:
        return
    for key in drop:
        e = _queue.pop(key, None)
        if e:
            _queued_set.discard((e.get("call_key", ""), int(e.get("x_val", 0)), e.get("type", "x")))
    try:
        _atomic_write_json(DIPBOT_QUEUE_FILE, _queue)
    except Exception as ex:
        logger.warning(f"dipbot: queue prune save failed: {ex}")
    logger.info(f"dipbot: pruned {len(drop)} queued dip entries")


async def _dipbot_check_one(call_key, call):
    """One dip check for one call: fetch live MC (verified), run the dip tick.

    Mirrors the dip-relevant part of monbot_check_one: bulk cache first,
    one bounded alt-source probe for young/parked calls only, verified ratio
    (FDV + spike guards), entry-confirmation fail-safe gate, then the dip
    tick. Never posts. Never writes main bot files.
    """
    try:
        if call.get("frozen"):
            return
        ca = call.get("ca") or ""
        dex = _get_cached_dex(ca) if ca else None
        if dex is None:
            # Bounded probe only for young calls or calls with parked dip
            # levels; older calls wait for the next bulk refresh (cheap).
            if _age_seconds(call) <= 300 or call.get("pending_dip"):
                try:
                    alt = await asyncio.to_thread(_fetch_alt_sources_sync, ca)
                except Exception:
                    alt = None
                if alt:
                    dex = alt
            if dex is None:
                return
        ratio = _verified_live_ratio(call, dex)
        try:
            cur_mc = float((dex or {}).get("mcap", 0) or 0)
        except (TypeError, ValueError):
            cur_mc = 0.0
        if cur_mc <= 0:
            try:
                entry_mc = float(call.get("entry_mc") or 0)
            except (TypeError, ValueError):
                entry_mc = 0.0
            cur_mc = entry_mc * ratio if entry_mc > 0 and ratio > 0 else 0.0
        if cur_mc <= 0 or ratio <= 0:
            return
        # Entry-confirmation gate (fail-safe 120s / 420s RH, as in monbot).
        if not call.get("entry_confirmed", True):
            gate_age = _age_seconds(call)
            gate_limit = 420 if str(call.get("chain") or "").upper() == "RH" else 120
            if gate_age <= gate_limit:
                return
        try:
            await _dipbot_dip_tick(call_key, call, cur_mc)
        except Exception as e_dip:
            logger.warning(f"dipbot: dip check skipped {call_key}: {e_dip}")
        _sync_call_to_dip(call_key, call)
    except Exception as e:
        logger.error(f"dipbot: check failed {call_key}: {e}")


async def _dipbot_tick():
    """One dip pass over all tracked calls (dip-only lane).

    Reloads data, filters to active + alive channels, buckets hot/warm/
    regular by age (hot = fresh or parked-dip calls, checked every tick),
    bulk-refreshes the price cache for the selected CAs, then checks them
    in batches of 30. Prunes the queue and saves dip state (throttled).
    """
    global _DIPBOT_TICK_N
    _DIPBOT_TICK_N += 1
    tick_n = _DIPBOT_TICK_N
    try:
        _dipbot_reload()
        items = list(_calls.items())
        if not items:
            return
        active_channels_lower = {c.lower() for c in load_channels()}
        items = [(k, v) for k, v in items if v.get("channel", "").lower() in active_channels_lower]
        items = [(k, v) for k, v in items if not v.get("rugged")]
        items = [(k, v) for k, v in items if not v.get("frozen")]
        if not items:
            return
        now_ts = time.time()
        hot, warm, regular = [], [], []
        for kv in items:
            k, c = kv
            try:
                if float(c.get("_glitch_until", 0) or 0) > now_ts:
                    continue
            except Exception:
                pass
            age = _age_seconds(c)
            if age <= HOT_WINDOW_SECONDS or (c.get("pending_dip") and age <= 300):
                hot.append(kv)
            elif age <= 600:
                warm.append(kv)
            else:
                regular.append(kv)
        hot.sort(key=lambda kv: _age_seconds(kv[1]))
        warm.sort(key=lambda kv: _age_seconds(kv[1]))
        selected = list(hot)
        if tick_n % WARM_EVERY_N_TICKS == 0:
            selected.extend(warm)
        # Regular calls: round-robin slice so every call is covered over time.
        if regular:
            start = ((tick_n - 1) * _DIPBOT_MAX_CHECKS_PER_TICK) % len(regular)
            selected.extend(regular[start:start + _DIPBOT_MAX_CHECKS_PER_TICK])
        selected = selected[:_DIPBOT_MAX_CHECKS_PER_TICK * 2]
        if not selected:
            return
        # Bulk-refresh the price cache for the selected CAs (threaded).
        chain_map = {}
        cas = []
        for k, c in selected:
            if c.get("ca"):
                cas.append(c["ca"])
                chain_map[c["ca"]] = str(c.get("chain") or "")
        if cas:
            chunks = [cas[i:i + 100] for i in range(0, len(cas), 100)]
            try:
                await asyncio.gather(*[
                    asyncio.to_thread(_bulk_refresh_dex_sync, ch, 0, chain_map)
                    for ch in chunks])
            except Exception as e:
                logger.warning(f"dipbot: bulk refresh failed: {e}")
        for i in range(0, len(selected), 30):
            await asyncio.gather(*[
                _dipbot_check_one(k, v) for k, v in selected[i:i + 30]])
        _maybe_save_dip()
        _dipbot_prune_queue()
        logger.info(f"dipbot: tick {tick_n} checked {len(selected)} calls "
                    f"(hot {len(hot)}, warm {len(warm)}, queue {len(_queue)})")
    except Exception as e:
        logger.error(f"dipbot: tick failed: {e}")


async def _dipbot_retry_parked():
    """15s retry sweep (the pending-media retry contract).

    Parked dip levels (no media set when they fired) are re-checked
    explicitly. The normal tick already re-evaluates them every pass, so
    this is the guaranteed 15s re-fire: the moment media is set for a
    parked level, the next sweep queues it. Nothing is ever posted
    text-only - a level with no media stays parked.
    """
    try:
        _dipbot_reload()
        parked = [(k, v) for k, v in _calls.items() if v.get("pending_dip")]
        if not parked:
            return
        logger.info(f"dipbot: retry sweep, {len(parked)} calls with parked dip levels")
        for k, v in parked:
            try:
                await _dipbot_check_one(k, v)
            except Exception as e:
                logger.warning(f"dipbot: retry check failed {k}: {e}")
        _maybe_save_dip(force=True)
    except Exception as e:
        logger.error(f"dipbot: retry sweep failed: {e}")


async def _dipbot_main(stop_event):
    _dipbot_load_own()
    _dipbot_reload()
    logger.info(f"dipbot: started, {_dipbot_tick.__name__} every "
                f"{DIPBOT_TICK_SECONDS}s, retry every {_DIPBOT_RETRY_SECONDS}s, "
                f"{len(_calls)} calls loaded, {len(_queue)} queued")
    tick_task = asyncio.create_task(_dipbot_tick_loop(stop_event))
    retry_task = asyncio.create_task(_dipbot_retry_loop(stop_event))
    await stop_event.wait()
    tick_task.cancel()
    retry_task.cancel()
    try:
        _maybe_save_dip(force=True)
        _atomic_write_json(DIPBOT_QUEUE_FILE, _queue)
    except Exception as e:
        logger.warning(f"dipbot: shutdown save failed: {e}")
    logger.info("dipbot: stopped")


async def _dipbot_tick_loop(stop_event):
    while not stop_event.is_set():
        t0 = time.time()
        try:
            await _dipbot_tick()
        except Exception as e:
            logger.error(f"dipbot: tick loop error: {e}")
        dt = time.time() - t0
        wait = max(0.2, DIPBOT_TICK_SECONDS - dt)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait)
        except asyncio.TimeoutError:
            pass


async def _dipbot_retry_loop(stop_event):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=_DIPBOT_RETRY_SECONDS)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            break
        try:
            await _dipbot_retry_parked()
        except Exception as e:
            logger.error(f"dipbot: retry loop error: {e}")


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    _load_dotenv(os.path.join(here, ".env"))
    os.makedirs(DIPBOT_DATA_DIR, exist_ok=True)
    stop_event = asyncio.Event()

    def _on_signal(signum, frame):
        logger.info(f"dipbot: signal {signum}, stopping")
        stop_event.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    try:
        asyncio.run(_dipbot_main(stop_event))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
