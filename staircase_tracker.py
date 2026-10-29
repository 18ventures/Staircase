"""
STAIRCASE TRACKER — standalone service (no trading code, no Orion dependency).

What it does, for BTC perps on Binance:
  * Reads 4H / 30m / 5m candles + mark price, works out the composite colour on each (same maths as the
    TradingView composite: lookback 100, z-score 100, extremes 90/10, Yellow=Orange, Blue=Green).
  * Logs every staircase setup (Short/Long A-E) the moment it fires.
  * Logs every full alignment: same colour on 4H, 30m and 5m (Red, Orange, Green, Cyan) and how long it lasted.
  * For every log: WPR, and the outcome against TP / SL (both directions for alignments).
  * Serves a password-protected live page for the Home Screen: /staircase

Needs: STAIRCASE_TOKEN (the page password). Use DATABASE_URL (Postgres) so history survives redeploys.
Optional: TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID + TRACKER_TELEGRAM_ALERTS=true for plain (no-button) alerts.
Run:  gunicorn staircase_tracker:app --workers 1 --threads 4 --bind 0.0.0.0:$PORT   (ONE worker only)
"""
import os, time, json, base64, hmac, threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, request as flask_request, redirect

# ── Settings ────────────────────────────────────────────────────────────────
SYMBOL        = os.environ.get("SYMBOL", "BTCUSDT")
FUTURES_BASE  = os.environ.get("FUTURES_BASE", "https://fapi.binance.com")
LOOKBACK      = int(os.environ.get("STAIRCASE_LOOKBACK", "100"))
ZLEN          = int(os.environ.get("STAIRCASE_ZLEN", os.environ.get("STAIRCASE_ZSCORE_LEN", "100")))
HIGH          = float(os.environ.get("STAIRCASE_EXTREME_HIGH", "90"))
LOW           = float(os.environ.get("STAIRCASE_EXTREME_LOW", "10"))
TF_H          = os.environ.get("STAIRCASE_TF_HIGH", "4h")
TF_M          = os.environ.get("STAIRCASE_TF_MID", "30m")
TF_L          = os.environ.get("STAIRCASE_TF_LOW", "5m")
TP_PCT        = float(os.environ.get("STAIRCASE_TP_PCT", "0.45"))                  # TP1
TP2_PCT       = float(os.environ.get("STAIRCASE_TP2_PCT", "0.55"))                  # TP2
TP2_MAX_HOURS = float(os.environ.get("STAIRCASE_TP2_MAX_HOURS", "5"))                # once TP2 is hit, a trade still short of TP3 is closed 5h after entry
KILL_R        = float(os.environ.get("STAIRCASE_KILL_R", "7"))                        # a setup is paused when its total R (current settings) reaches -7R
INS_MIN_N     = int(os.environ.get("STAIRCASE_INSIGHT_MIN_N", "15"))                  # finished trades needed before promote / retire is suggested
PROMOTE_AVG_R = float(os.environ.get("STAIRCASE_PROMOTE_AVG_R", "0.3"))
RETIRE_AVG_R  = float(os.environ.get("STAIRCASE_RETIRE_AVG_R", "-0.2"))
TP3_R         = float(os.environ.get("STAIRCASE_TP3_R", "2"))                        # TP3 as a multiple of the stop (2 = 2R)
BE_AFTER_TP1  = os.environ.get("STAIRCASE_BE_AFTER_TP1", "true").lower() == "true"   # once TP1 is hit, price touching the entry closes the trade as "TP1 only"
CTX_TF        = os.environ.get("STAIRCASE_CTX_TF", "30m")
WPR_LEN       = int(os.environ.get("STAIRCASE_WPR_LEN", "200"))
LIVE_HIGH     = os.environ.get("STAIRCASE_LIVE_HIGH", "true").lower() == "true"   # false = slow TF uses closed candles only
LIVE_MID      = os.environ.get("STAIRCASE_LIVE_MID", "false").lower() == "true"    # default false: mid TF (30m) uses closed candles only
TIMEOUT_SECS  = 24 * 3600
TOKEN         = os.environ.get("STAIRCASE_TOKEN", "") or os.environ.get("READ_API_SECRET", "")
DATABASE_URL  = os.environ.get("DATABASE_URL", "")
# A second tracker (e.g. a 10m test) can share the same database: give it its own prefix so the two never touch each other's saved data.
STORE_PREFIX  = os.environ.get("STAIRCASE_STORE_PREFIX", "")
NAME          = os.environ.get("STAIRCASE_NAME", "")      # shown in the page header / tab title, e.g. "10m test"
DATA_DIR      = os.environ.get("DATA_DIR", "./data")
TG_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT       = os.environ.get("TELEGRAM_CHAT_ID", "")
TG_ALERTS     = os.environ.get("TRACKER_TELEGRAM_ALERTS", "false").lower() == "true"
MAX_HISTORY   = 3000
# Days nothing is logged or shown (Mon=0 ... Sat=5 ... Sun=6), judged on UK time. Default: Saturday only.
SKIP_DAYS = {int(x) for x in os.environ.get("STAIRCASE_SKIP_DAYS", "5").replace(" ", "").split(",") if x != ""}
SKIP_TZ   = os.environ.get("STAIRCASE_SKIP_TZ", "Europe/London")


LEVERAGE = float(os.environ.get("STAIRCASE_LEVERAGE", "50"))   # only used to show account % next to price %
# The stop is a fixed loss on margin: 20% of margin at 50x = 0.40% on price. R is always measured against this stop.
SL_ROI = float(os.environ.get("STAIRCASE_SL_ROI", "20"))
SL_PCT = round(SL_ROI / LEVERAGE, 4)
TP3_PCT = round(SL_PCT * TP3_R, 4)   # 2R at a 0.40% stop = 0.80% on price
ZONE_FILTER = os.environ.get("STAIRCASE_ZONE_FILTER", "true").lower() == "true"
ZONE_RANGE  = "mid5" if os.environ.get("STAIRCASE_ZONE_RANGE", "30m") == "5m" else "mid30"   # which midline decides


def offzone(e):
    """A long taken with price ABOVE the midline, or a short with price BELOW it, is not a true discount/premium
    entry. It is still logged, but kept in its own 'off-zone' results so it never mixes with the real ones."""
    if not ZONE_FILTER:
        return False
    side = next(iter(e.get("res") or {}), None)
    ms = e.get(ZONE_RANGE + "_side")
    return (side == "long" and ms == "above") or (side == "short" and ms == "below")


def skipday(ts=None):
    try:
        return datetime.fromtimestamp(ts if ts is not None else time.time(), ZoneInfo(SKIP_TZ)).weekday() in SKIP_DAYS
    except Exception:
        return False

TF_SECS = {"1m": 60, "3m": 180, "5m": 300, "10m": 600, "15m": 900, "30m": 1800, "1h": 3600,
           "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200}

# Each slow timeframe is read from its last CLOSED candle, except near the end of the candle, when it reads the forming one.
# 30m: live in the final 5 minutes. 4H: live in the final 90 minutes. Once a candle closes, whatever it closed as is what counts.
MID_LIVE_LAST_MIN  = float(os.environ.get("STAIRCASE_MID_LIVE_LAST_MIN", "5"))
HIGH_LIVE_LAST_MIN = float(os.environ.get("STAIRCASE_HIGH_LIVE_LAST_MIN", "90"))


def _in_last(tf, minutes, now=None):
    secs = TF_SECS.get(tf, 1800)
    return ((now if now is not None else time.time()) % secs) >= secs - minutes * 60


def mid_live(now=None):
    return True if LIVE_MID else _in_last(TF_M, MID_LIVE_LAST_MIN, now)


def high_live(now=None):
    return False if not LIVE_HIGH else _in_last(TF_H, HIGH_LIVE_LAST_MIN, now)   # STAIRCASE_LIVE_HIGH=false = closed 4H candles only



def _lbl(tf):
    return tf[:-1] + ("H" if tf.endswith("h") else "m")


LABELS = (_lbl(TF_H), _lbl(TF_M), _lbl(TF_L))
EMOJI = {"Red": "🟥", "Orange": "🟧", "Grey": "⬜", "Green": "🟩", "Cyan": "🟦"}

# (direction, letter, slow colour, mid colour, fast colour BEFORE, fast colour NOW)
RULES = [
    ("short", "A++", "Red",    "Orange", "Red",    "Orange"),
    ("short", "B", "Red",    "Orange", "Orange", "Grey"),
    ("short", "A+", "Orange", "Red",    "Red",    "Orange"),
    ("short", "B+", "Orange", "Red",   "Orange", "Grey"),
    ("short", "D", "Green",  "Green",  "Orange", "Grey"),
    ("long",  "A++", "Cyan",   "Green",  "Cyan",   "Green"),
    ("long",  "B", "Cyan",   "Green",  "Green",  "Grey"),
    ("long",  "A+", "Green",  "Cyan",   "Cyan",   "Green"),
    ("long",  "B+", "Green",  "Cyan",   "Green",  "Grey"),
    ("long",  "D", "Orange", "Orange", "Green",  "Grey"),
    ("short", "G", "Cyan",   "Orange", "Red",    "Orange"),
    ("short", "H", "Cyan",   "Orange", "Grey",   "Green"),
    ("short", "E", "Green",  "Green",  "Green",  "Grey"),
    ("short", "F", "Orange", "Orange", "Orange", "Orange"),
    ("short", "K", "Green",  "Red",    "Red",    "Orange"),
    ("short", "L", "Green",  "Orange", "Orange", "Grey"),
    ("short", "M", "Green",  "Green",  "Grey",   "Green"),
    ("long",  "M", "Orange", "Orange", "Grey",   "Orange"),
    ("long",  "K", "Orange", "Cyan",   "Cyan",   "Green"),
    ("long",  "L", "Orange", "Green",  "Green",  "Grey"),
    ("long",  "F", "Green",  "Green",  "Green",  "Green"),
    ("long",  "E", "Orange", "Orange", "Orange", "Grey"),
    ("long",  "G", "Red",    "Green",  "Cyan",   "Green"),
    ("long",  "H", "Red",    "Green",  "Grey",   "Orange"),
    ("long",  "J", "Green",  "Green",  "Orange", "Red"),
    ("short", "J", "Orange", "Orange", "Green",  "Cyan"),
]
NOTES = {
    ("short", "A++"): "Early entry, best R",   ("long", "A++"): "Early entry, best R",
    ("short", "B"): "Final confirmation",    ("long", "B"): "Final confirmation",
    ("long", "A+"): "Test, possibly the best signal: reversal off a Cyan 30m",
    ("short", "A+"): "Test, possibly the best signal: reversal off a Red 30m",
    ("long", "B+"): "Test, possibly the best signal: Cyan 30m, 5m fades to Grey",
    ("short", "B+"): "Test, possibly the best signal: Red 30m, 5m fades to Grey",
    ("short", "D"): "Last premium short in a heavy downtrend: price pushes down and holds, 5m Orange to Grey, above the 30m and 5m midlines", ("long", "D"): "Last discount long in a heavy premium trend: price pushes up and holds, 5m Green to Grey, below the 30m and 5m midlines",
    ("long", "G"): "Lowest discount in a strong higher timeframe",
    ("long", "H"): "2nd opportunity: last discount in a premium trend before the uptrend is lost",
    ("short", "G"): "Highest premium in a strong higher timeframe",
    ("short", "K"): "Green 4H, Red 30m: the 5m turns down off Red",
    ("short", "L"): "Green 4H, Orange 30m: the 5m fades from Orange to Grey",
    ("short", "M"): "Trend continuation: 4H and 30m both Green, the 5m dips back into Green after a bounce to Grey (one step after Short E)",
    ("long", "M"): "Trend continuation: 4H and 30m both Orange, the 5m pushes back into Orange after a dip to Grey (one step after Long E)",
    ("long", "K"): "Orange 4H, Cyan 30m: the 5m turns up off Cyan",
    ("long", "L"): "Orange 4H, Green 30m: the 5m fades from Green to Grey",
    ("long", "J"): "HTF discount, LTF showing strong strength, recovering the Red to Orange to Red trend",
    ("short", "J"): "HTF premium, LTF showing strong weakness, recovering the Cyan to Green to Cyan trend",
    ("long", "F"): "All Green long, only when price is below the midline on all three timeframes (4H, 30m and 5m) and the 5m composite is above 15 but not above 30",
    ("short", "F"): "All Orange short, only when price is above the midline on all three timeframes (4H, 30m and 5m) and the 5m composite is below 85",
    ("short", "E"): "Fading the bounce: 5m climbs out of Green into Grey with 4H and 30m both Green (the old Long C, flipped)",
    ("long", "E"): "Flip of the retired Short C: 5m drops from Orange into Grey with 4H and 30m both Orange",
    ("short", "H"): "2nd opportunity: last premium in a discount trend before the downtrend is lost",
}
for _d, _L, _h, _m, _p, _l in RULES:   # tag each setup with its 4H/30m family so look-alikes (e.g. D, E, M) are easy to tell apart
    if (_d, _L) in NOTES and not str(NOTES[(_d, _L)]).startswith(_h + "/"):
        NOTES[(_d, _L)] = f"{_h}/{_m} family · {NOTES[(_d, _L)]}"
# UNTESTED setups: ones you think are the best but have no results yet. Logged and scored like the others,
# but kept out of the main Setups tab, the Results totals and the + comparison.
# Numbered per side (Short U1, Short U2 ... and Long U1, Long U2 ...). "*" after the number = Claude-picked, no star = yours.
# To add one, use the next free number on that side and never reuse a number, so old results never point at the wrong setup.
# (direction, id, slow colour, mid colour, fast colour BEFORE, fast colour NOW)
UNTESTED = [
    ("long", "U1", "Cyan", "Cyan", "Cyan", "Green"),
    ("long", "U2", "Cyan", "Cyan", "Grey", "Orange"),
    ("long", "U4", "Grey", "Orange", "Grey", "Orange"),
    ("short", "U8", "Grey", "Green", "Grey", "Green"),
    ("long", "U10", "Green", "Green", "Grey", "Orange"),
    ("short", "U9", "Orange", "Orange", "Grey", "Green"),
    ("short", "U12", "Green", "Red", "Orange", "Grey"),
    ("long", "U13", "Orange", "Cyan", "Green", "Grey"),
]
UNOTES = {
    ("long", "U1"): f"{LABELS[0]} Cyan, {LABELS[1]} Cyan, {LABELS[2]} Cyan → Green",
    ("long", "U2"): f"{LABELS[0]} Cyan, {LABELS[1]} Cyan, {LABELS[2]} Grey → Orange",
    ("long", "U4"): f"{LABELS[0]} Grey, {LABELS[1]} Orange, {LABELS[2]} Grey → Orange",
    ("short", "U8"): f"{LABELS[0]} Grey, {LABELS[1]} Green, {LABELS[2]} Grey → Green",
    ("long", "U10"): f"{LABELS[0]} Green, {LABELS[1]} Green, {LABELS[2]} Grey → Orange, only with the 5m WPR below -25",
    ("short", "U9"): f"{LABELS[0]} Orange, {LABELS[1]} Orange, {LABELS[2]} Grey → Green, only with the 5m WPR above -75",
    ("short", "U12"): f"{LABELS[0]} Green, {LABELS[1]} Red, {LABELS[2]} Orange → Grey",
    ("long", "U13"): f"{LABELS[0]} Orange, {LABELS[1]} Cyan, {LABELS[2]} Green → Grey",
}
# Untested setups allowed to log even though their state is on the Blocked list (the block still stops every real setup and alignment).
# Long U10 sits in Green/Green/Orange (blocked both ways) and Long J ends on a 5m Red (longs on a Red 5m are blocked),
# so without this they could never fire. (Long J was untested Long U11 until it graduated.)
BLOCK_EXEMPT = {("long", "U10"), ("long", "J"), ("short", "J"), ("short", "L")}   # Short J ends on a 5m Cyan (shorts on a Cyan 5m are blocked)
# "*" setups: Claude-picked. Every sensible leftover state (30m on the trade's side, the usual 5m turn), so a good one doesn't
# sit unnoticed. Removed for being impossible or clashing with a main setup: their numbers are retired, not reused.
CLAUDE_PICKS = [
    ("short", "U1*", "Red", "Red", "Red", "Orange"),
    ("short", "U2*", "Red", "Red", "Orange", "Grey"),
    ("short", "U3*", "Orange", "Orange", "Red", "Orange"),
    ("short", "U4*", "Grey", "Orange", "Red", "Orange"),
    ("short", "U5*", "Grey", "Orange", "Orange", "Grey"),
    ("short", "U6*", "Cyan", "Orange", "Orange", "Grey"),
    ("long", "U5*", "Red", "Green", "Green", "Grey"),
    ("long", "U6*", "Grey", "Green", "Cyan", "Green"),
    ("long", "U7*", "Grey", "Green", "Green", "Grey"),
    ("long", "U8*", "Green", "Green", "Cyan", "Green"),
    ("long", "U9*", "Cyan", "Cyan", "Green", "Grey"),
]
for _d, _id, _h, _m, _p, _l in CLAUDE_PICKS:
    UNTESTED.append((_d, _id, _h, _m, _p, _l))
    UNOTES[(_d, _id)] = f"* Claude-picked: {LABELS[0]} {_h}, {LABELS[1]} {_m}, {LABELS[2]} {_p} → {_l}"

# Setups whose 30m colour is Grey are discarded: not logged, not shown, not counted (existing logs are hidden too).
DROP_MID_GREY = os.environ.get("STAIRCASE_DROP_MID_GREY", "true").lower() == "true"
if DROP_MID_GREY:
    RULES = [r for r in RULES if r[3] != "Grey"]
    UNTESTED = [r for r in UNTESTED if r[3] != "Grey"]


# A++ "pocket": the 30m WPR band you expect a true A++ to sit in. Never blocks a trade, only tagged and scored separately.
# Long: -85 to -75. Short: the mirror, -25 to -15.
POCKET = {"long": (-85.0, -75.0), "short": (-25.0, -15.0)}
POCKET_KEYS = ("long_A++", "short_A++")


def in_pocket(e):
    w = e.get("wpr")
    if w is None or e.get("key") not in POCKET_KEYS:
        return None
    lo, hi = POCKET[e["key"].split("_")[0]]
    return lo <= float(w) <= hi


def hidden(e):
    """Entries that are never shown or counted: logged on a skipped day, or a setup that fired with a Grey 30m."""
    if skipday(e["t"]):
        return True
    if len(e.get("cols", [])) > 2 and ((e.get("key") == "short_D" and e["cols"][2] != "Orange") or (e.get("key") == "long_D" and e["cols"][2] != "Green")):
        return True      # an earlier draft of Short D (5m Green to Grey) that was never meant to run
    return DROP_MID_GREY and e.get("kind") in ("setup", "untested") and len(e.get("cols", [])) > 1 and e["cols"][1] == "Grey"


def retired_version(e):
    """True for a logged setup whose rule no longer exists, or whose colours have since changed: the letter is still
    there but it was a different trade when this fired, so it is labelled "(retired version)" and kept out of the current stats."""
    if e.get("kind") not in ("setup", "untested"):
        return False
    try:
        d, L = e["key"].split("_", 1)
        for rd, rl, rh, rm, rp, rn in RULES + UNTESTED:
            if (rd, rl) == (d, L):
                c = tuple(e.get("cols", []))
                if len(c) == 4 and (d, L) in EPISODE:      # a state, not a 5m turn: the "before" colour doesn't matter
                    return (c[0], c[1], c[3]) != (rh, rm, rn)
                return c != (rh, rm, rp, rn)
    except Exception:
        return False
    return True


# Which colours each setup letter had, and the last time that version was live. Survives "Clear history".
RULE_LOG = {"cur": {}, "retired": []}      # cur: key -> {cols, last}; retired: [{key, cols, last}]
_rule_saved = [0.0]


def touch_rules(now=None, force=False):
    """Run about once a minute. Notes the colours every current rule has; when a rule's colours change, or a rule is
    removed, the old version is filed under 'retired' with the last time it was live."""
    now = now or time.time(); changed = False
    live = {f"{d}_{L}": [h, m, p, n] for d, L, h, m, p, n in RULES + UNTESTED}
    for k, rec in list(RULE_LOG["cur"].items()):
        if live.get(k) != rec["cols"]:
            RULE_LOG["retired"].append({"key": k, "cols": rec["cols"], "last": rec["last"]}); del RULE_LOG["cur"][k]; changed = True
    for k, cols in live.items():
        if k not in RULE_LOG["cur"]:
            changed = True
        RULE_LOG["cur"][k] = {"cols": cols, "last": now}
    if changed or force or now - _rule_saved[0] > 3600:
        _rule_saved[0] = now
        store_set("rule_log", json.dumps(RULE_LOG))


def _fmt_day(t):
    return datetime.fromtimestamp(t, ZoneInfo("Europe/London")).strftime("%d %b").lstrip("0")


def last_used(key, cols):
    """Last time this exact version (key + colours) was live or fired, as '18 Oct', or None if unknown."""
    cols = list(cols); best = 0.0
    for r in RULE_LOG["retired"]:
        if r["key"] == key and r["cols"] == cols:
            best = max(best, r["last"])
    for e in HISTORY:
        if e["key"] == key and list(e.get("cols", [])) == cols:
            best = max(best, e["t"])
    return _fmt_day(best) if best else None


ALIGN_COLOURS = ["Red", "Orange", "Green", "Cyan"]   # all three timeframes the same colour (Grey excluded)
ALIGN_DIR = {"Red": "short", "Orange": "short", "Green": "long", "Cyan": "long"}   # the trade each alignment represents


def cfg_sig():
    base = f"{LOOKBACK}/{ZLEN}/{HIGH:g}/{LOW:g}"
    base = base if (TF_H, TF_M, TF_L) == ("4h", "30m", "5m") else f"{'/'.join(LABELS)}|{base}"
    mode = ("" if LIVE_HIGH else "H") + ("" if LIVE_MID else "M")
    return base + (f"|closed-{mode}" if mode else "")   # closed-candle modes keep their own counts


# ── Storage: Postgres if DATABASE_URL is set, otherwise a JSON file ─────────────
_db_ready = False


def _db():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


def _init_db():
    global _db_ready
    if _db_ready or not DATABASE_URL:
        return
    conn = _db(); cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tracker_state (key TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())")
    conn.commit(); cur.close(); conn.close()
    _db_ready = True


STORAGE = {"mode": "postgres" if DATABASE_URL else "file", "ok": True, "error": "", "loaded": False}


def store_read(key):
    """Raises on any failure, so a broken read can never be mistaken for 'nothing saved yet'."""
    if DATABASE_URL:
        _init_db()
        conn = _db(); cur = conn.cursor()
        cur.execute("SELECT value FROM tracker_state WHERE key=%s", (STORE_PREFIX + key,))
        row = cur.fetchone(); cur.close(); conn.close()
        return row[0] if row else None
    p = os.path.join(DATA_DIR, f"{STORE_PREFIX}{key}.json")
    return open(p).read() if os.path.exists(p) else None


def store_get(key):
    try:
        return store_read(key)
    except Exception as e:
        print(f"[Store] read error {key}: {e}")
        return None


def store_set(key, value):
    try:
        if DATABASE_URL:
            _init_db()
            conn = _db(); cur = conn.cursor()
            cur.execute("INSERT INTO tracker_state (key,value,updated_at) VALUES (%s,%s,NOW()) "
                        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()", (STORE_PREFIX + key, value))
            conn.commit(); cur.close(); conn.close()
        else:
            os.makedirs(DATA_DIR, exist_ok=True)
            tmp = os.path.join(DATA_DIR, f"{STORE_PREFIX}{key}.json.tmp")
            open(tmp, "w").write(value)
            os.replace(tmp, os.path.join(DATA_DIR, f"{STORE_PREFIX}{key}.json"))
        STORAGE["ok"] = True; STORAGE["error"] = ""
    except Exception as e:
        STORAGE["ok"] = False; STORAGE["error"] = str(e)[:200]
        print(f"[Store] write error {key}: {e}")


# ── State ────────────────────────────────────────────────────────────────────
LOCK = threading.RLock()
HISTORY = []          # newest last
SINCE = {}            # cfg -> time tracking began
STATE = {"snap": None, "why": "starting…", "last_bar": None, "updated": 0}


def save_history():
    if not STORAGE["loaded"]:      # never overwrite saved history with an empty list after a failed load
        return
    store_set("history", json.dumps(HISTORY[-MAX_HISTORY:]))


def save_since():
    if not STORAGE["loaded"]:
        return
    store_set("since", json.dumps(SINCE))


# ── Indicators ──────────────────────────────────────────────────────────────
def composite(closes):
    """Same maths as the TradingView composite."""
    if len(closes) < ZLEN:
        return None
    w = closes[-ZLEN:]
    mean = sum(w) / len(w)
    sd = (sum((x - mean) ** 2 for x in w) / len(w)) ** 0.5
    if sd == 0:
        return None
    zn = min(100.0, max(0.0, 50 + ((closes[-1] - mean) / sd) * 16.67))
    last = closes[-1]
    rank = sum(1 for i in range(LOOKBACK) if last > closes[-1 - i])
    return (zn + rank / LOOKBACK * 100) / 2


def colour(v):
    if v >= HIGH: return "Red"
    if v >= 55: return "Orange"
    if v >= 45: return "Grey"
    if v >= LOW: return "Green"
    return "Cyan"


def wpr(highs, lows, closes, length):
    """Orion's Williams %R (length 200, 30m): typical price (H+L+C)/3, last `length` candles. -100 (low) .. 0 (high)."""
    hh, ll = max(highs[-length:]), min(lows[-length:])
    tp = (highs[-1] + lows[-1] + closes[-1]) / 3
    return -50.0 if hh == ll else (hh - tp) / (hh - ll) * -100


# Extra conditions a setup needs on top of its colours: (side, letter) -> {ctx field: required value}.
# Short D is only valid with price above both the 30m and the 5m midline. Missing data means it does not fire.
SETUP_CONDS = {("short", "F"): {"mid4h_side": "above", "mid30_side": "above", "mid5_side": "above"},
               ("long", "F"): {"mid4h_side": "below", "mid30_side": "below", "mid5_side": "below"},
               ("short", "D"): {"mid30_side": "above", "mid5_side": "above"},
               ("long", "D"): {"mid30_side": "below", "mid5_side": "below"}}


# Minimum composite a setup needs (strictly above), read live: (side, letter) -> {ctx field: minimum}. Missing data means it does not fire.
# Long F: the 5m composite must be above 15, so it is not firing right on the edge of Cyan.
COMP_MIN = {("long", "F"): {"comp_l": float(os.environ.get("STAIRCASE_LONG_F_5M_MIN", "15"))}}


# Cap (inclusive: it may equal it, but not go above): Long F's 5m composite can't be above 30.
COMP_CAP = {("long", "F"): {"comp_l": float(os.environ.get("STAIRCASE_LONG_F_5M_MAX", "30"))}}


# Floor (inclusive: it may equal it, but not go below): Short F's 5m composite can't be below 70 (the mirror of Long F's cap of 30).
COMP_FLOOR = {("short", "F"): {"comp_l": float(os.environ.get("STAIRCASE_SHORT_F_5M_MIN", "70"))}}


# Maximum composite (strictly below): Short F, the mirror, needs the 5m composite below 85 so it is not on the edge of Red.
COMP_MAX = {("short", "F"): {"comp_l": float(os.environ.get("STAIRCASE_SHORT_F_5M_MAX", "85"))}}


# Setups that skip the global midline/WPR filter (Short D has its own conditions; Short B is exempt for now).
GATE_EXEMPT = {("short", "B"), ("short", "D"), ("short", "E"), ("long", "E"), ("short", "F"), ("long", "F"), ("long", "U10"), ("short", "U9"), ("short", "M"), ("long", "M")}   # E: no filters, as before; M follows E


# All Green only counts when price is below the 30m midline AND the 5m WPR is below this line (weak "longs" are cut out).
GREEN_BAND = (float(os.environ.get("STAIRCASE_GREEN_BAND_LOW", "-85")), float(os.environ.get("STAIRCASE_GREEN_BAND_HIGH", "-75")))   # 5m WPR band a Green-30m long must sit in
ALLGREEN_WPR = GREEN_BAND[1]   # (kept for the text below)


# All Orange is the mirror: only counts when price is above the 30m midline AND the 5m WPR is above this line.
ALLORANGE_WPR = float(os.environ.get("STAIRCASE_ALLORANGE_WPR", "-25"))
ALLORANGE_5M_MIN = float(os.environ.get("STAIRCASE_ALLORANGE_5M_MIN", "70"))   # All Orange: the 5m composite can't be below this (same floor as Short F)
ALLGREEN_5M_MAX = float(os.environ.get("STAIRCASE_ALLGREEN_5M_MAX", "30"))   # All Green: the 5m composite can't be above this (same cap as Long F)


def align_cond_ok(ac, ctx):
    if ac not in ("Green", "Orange"):
        return True
    if not ctx or ctx.get("wpr5") is None:
        return False
    w = float(ctx["wpr5"])
    if ac == "Green":
        return (ctx.get("mid30_side") == "below" and GREEN_BAND[0] <= w <= GREEN_BAND[1]
                and ctx.get("comp_l") is not None and float(ctx["comp_l"]) <= ALLGREEN_5M_MAX)
    return (ctx.get("mid30_side") == "above" and w > ALLORANGE_WPR
            and ctx.get("comp_l") is not None and float(ctx["comp_l"]) >= ALLORANGE_5M_MIN)


# Per-setup 5m WPR limit (strictly): (side, id) -> ("below"/"above", level). Missing WPR means it does not fire.
# Long U10 only counts with the 5m WPR below -25; its mirror, Short U9, only with the 5m WPR above -75.
WPR5_LIMIT = {("long", "U10"): ("below", -25.0), ("short", "U9"): ("above", -75.0)}


def cond_ok(d, L, ctx):
    lim = WPR5_LIMIT.get((d, L))
    if lim:
        w = (ctx or {}).get("wpr5")
        if w is None or not (float(w) < lim[1] if lim[0] == "below" else float(w) > lim[1]):
            return False
    need = SETUP_CONDS.get((d, L))
    if need and not (bool(ctx) and all(ctx.get(k) == v for k, v in need.items())):
        return False
    for k, lo in (COMP_MIN.get((d, L)) or {}).items():
        if not ctx or ctx.get(k) is None or float(ctx[k]) <= lo:
            return False
    for k, cap in (COMP_CAP.get((d, L)) or {}).items():
        if not ctx or ctx.get(k) is None or float(ctx[k]) > cap:
            return False
    for k, fl in (COMP_FLOOR.get((d, L)) or {}).items():
        if not ctx or ctx.get(k) is None or float(ctx[k]) < fl:
            return False
    for k, hi in (COMP_MAX.get((d, L)) or {}).items():
        if not ctx or ctx.get(k) is None or float(ctx[k]) >= hi:
            return False
    return True


# Blocked combos: (side, 4H, 30m, 5m now, reason, date added). Also listed on the Blocked tab.
# Blocked combos: (side, 4H, 30m, 5m now). A setup/alignment in this direction is never logged or alerted in that state.
# Add more rows here as impossible/poor states are identified.
BLOCKED = [
    ("short", "*", "Grey", "*", "30m is Grey: no trade in either direction until you decide to trade off the 5m and 4H alone", "25 Oct"),
    ("long",  "*", "Grey", "*", "30m is Grey: no trade in either direction until you decide to trade off the 5m and 4H alone", "25 Oct"),
    ("short", "*", "*", "Cyan", "5m already at the floor: shorting a move that's already done", "24 Oct"),
    ("long", "*", "*", "Red", "5m already at the top: mirror of the short block", "24 Oct"),
    ("short", "*", "*", "Red", "5m at the top: you can't know where the top is, too risky to short into it", "25 Oct"),
    ("long", "*", "*", "Cyan", "5m at the floor: you can't know where the bottom is, too risky to long into it", "25 Oct"),
    ("short", "Cyan", "Green", "Green", "4H Cyan / 30m Green: shorts only allowed on a Grey or Orange 5m", "24 Oct"),
    ("short", "Cyan", "Green", "Cyan", "4H Cyan / 30m Green: shorts only allowed on a Grey or Orange 5m", "24 Oct"),
    ("short", "Red", "Red", "Red", "All Red: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Red", "Red", "Red", "All Red: no trade either way (logged as a count only)", "24 Oct"),
    ("short", "Green", "Green", "Orange", "5m overshoot above two Green timeframes: no trade either way", "24 Oct"),
    ("long", "Green", "Green", "Orange", "5m overshoot above two Green timeframes: no trade either way", "24 Oct"),
    ("short", "Cyan", "Cyan", "Cyan", "All Cyan: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Cyan", "Cyan", "Cyan", "All Cyan: no trade either way (logged as a count only)", "24 Oct"),
    ("long", "Cyan", "Orange", "Orange", "4H Cyan / 30m Orange / 5m Orange: no long, manual trades included", "25 Oct"),
    ("long", "Cyan", "Orange", "Green", "4H Cyan / 30m Orange / 5m Green: no long, manual trades included", "25 Oct"),
    ("short", "Red", "Green", "Green", "4H Red / 30m Green / 5m Green: no short, manual trades included", "25 Oct"),
    ("short", "Red", "Green", "Orange", "4H Red / 30m Green / 5m Orange: no short, manual trades included", "25 Oct"),
    ("long", "Red", "Orange", "Orange", "Opposite side of the Short A++ state", "24 Oct"),
    ("short", "Green", "Orange", "Grey", "4H Green / 30m Orange / 5m Grey: weak counter-4H backdrop, no trade either way", "25 Oct"),
    ("long", "Green", "Orange", "Grey", "4H Green / 30m Orange / 5m Grey: weak counter-4H backdrop, no trade either way", "25 Oct"),
]


# Conditional blocks, judged on live context rather than colours alone: (side, condition text, reason, date added).
SHORT_GATE_WPR = float(os.environ.get("STAIRCASE_SHORT_GATE_WPR", "-25"))
OVERSOLD_WPR = float(os.environ.get("STAIRCASE_LONG_OVERSOLD_WPR", "-85"))   # 30m WPR below this blocks longs unless the 5m is back above it
LONG_GATE_WPR = float(os.environ.get("STAIRCASE_LONG_GATE_WPR", "-75"))   # mirror of the short line (-25 <-> -75)
BLOCKED_IF = [
    ("short", f"All Orange alignment, unless price is above the 30m midline AND the 5m WPR is above {ALLORANGE_WPR:g} AND the 5m composite is at least {ALLORANGE_5M_MIN:g}",
     "Triple Orange is not a good short on its own: only count it when price is high (above the 30m midline) and the 5m is stretched enough (above the line)", "25 Oct"),
    ("long", f"30m is Green and price is below the 30m midline, unless the 5m WPR is between {GREEN_BAND[0]:g} and {GREEN_BAND[1]:g}",
     "A long off a Green 30m below its midline only counts when the 5m WPR sits in the band: not too weak, not too strong", "25 Oct"),
    ("long", f"All Green alignment, unless price is below the 30m midline AND the 5m WPR is between {GREEN_BAND[0]:g} and {GREEN_BAND[1]:g} AND the 5m composite is no higher than {ALLGREEN_5M_MAX:g}",
     "Triple Green is not a good long on its own: only count it when price is low (below the 30m midline) and the 5m WPR is in the band", "25 Oct"),
    ("long", f"30m WPR below {OVERSOLD_WPR:g} unless the 5m WPR is above {OVERSOLD_WPR:g}",
     "Don't buy a move that is already extremely oversold on the 30m, unless the 5m has already turned back up", "25 Oct"),
    ("long", f"price above the 30m midline AND 5m WPR below {LONG_GATE_WPR:g}",
     "Don't long inside an already heavy uptrend: only the very bottom of it (5m WPR at or below the line, or price below the 30m midline)", "25 Oct"),
    ("short", f"price below the 30m midline AND 5m WPR above {SHORT_GATE_WPR:g} (Short B, D, E and F are exempt)",
     "Don't short inside an already heavy downtrend: only the very top of it (5m WPR at or above the line, or price above the 30m midline)", "25 Oct"),
]


def short_gated(ctx):
    """True when a short must not be taken: price below the 30m midline and 5m WPR above the line. Needs both readings."""
    return bool(ctx) and ctx.get("mid30_side") == "below" and ctx.get("wpr5") is not None and float(ctx["wpr5"]) > SHORT_GATE_WPR


def long_gated(ctx):
    """True when a long must not be taken: price above the 30m midline and 5m WPR below the line."""
    return bool(ctx) and ctx.get("mid30_side") == "above" and ctx.get("wpr5") is not None and float(ctx["wpr5"]) < LONG_GATE_WPR


def green30_gated(cols, ctx):
    """True when a long off a Green 30m must not be taken: price below the 30m midline and the 5m WPR outside the band. Needs both readings."""
    if cols[1] != "Green" or not ctx or ctx.get("mid30_side") != "below" or ctx.get("wpr5") is None:
        return False
    return not (GREEN_BAND[0] <= float(ctx["wpr5"]) <= GREEN_BAND[1])


def oversold_gated(ctx):
    """True when a long must not be taken: 30m WPR below the line and the 5m WPR not back above it. Needs both readings."""
    return bool(ctx) and ctx.get("wpr") is not None and ctx.get("wpr5") is not None and float(ctx["wpr"]) < OVERSOLD_WPR and float(ctx["wpr5"]) <= OVERSOLD_WPR


def is_blocked(side, h, m, l):
    """True if (side, 4H, 30m, 5m now) matches a BLOCKED row; "*" in a row matches any colour."""
    return any(b[0] == side and all(x in ("*", y) for x, y in zip(b[1:4], (h, m, l))) for b in BLOCKED)


# Blocked only when the 5m got there from a particular colour: (side, 4H, 30m, 5m BEFORE, 5m now, reason, date added).
BLOCKED_FROM = [
    ("long", "Green", "Orange", "Red", "Orange", "4H Green / 30m Orange / 5m Red→Orange: the 5m only just came off the top, no long", "25 Oct"),
]


def is_blocked_from(side, h, m, p, l):
    return any(b[0] == side and (b[1], b[2], b[3], b[4]) == (h, m, p, l) for b in BLOCKED_FROM)


def blocked(side, cols):
    return is_blocked(side, cols[0], cols[1], cols[3]) or is_blocked_from(side, *cols)


# "Episode" setups (Long F = All Green, Short F = All Orange): not a 5m turn, but a state. Fired once when the colours AND the
# midline conditions first line up, and not again until they stop lining up.
EPISODE = {("long", "F"): "Green", ("short", "F"): "Orange"}
EP_ACTIVE = set()


def episode_hits(cols, ctx):
    return [(d, L) for (d, L), c in EPISODE.items()
            if cols[0] == cols[1] == cols[3] == c and cond_ok(d, L, ctx) and not blocked(d, cols)]


def detect(cols, rules=None):
    return [(d, L) for (d, L, rh, rm, rp, rl) in (rules or RULES)
            if (d, L) not in EPISODE and tuple(cols) == (rh, rm, rp, rl) and ((d, L) in BLOCK_EXEMPT or not blocked(d, cols))]


# "Close to firing" lists setups up to this many steps away. A step = one of 4H, 30m, or the 5m before→now pair being different (so 1 to 3).
CLOSE_MAX_STEPS = int(os.environ.get("STAIRCASE_CLOSE_MAX_STEPS", "2"))


_CIDX = {"Cyan": 0, "Green": 1, "Grey": 2, "Orange": 3, "Red": 4}


def _candles_to(p, l, rp, rl):
    """Closed 5m candles still needed before the pair (p -> l) can read (rp -> rl): walk l to rp one colour per candle, then rp to rl."""
    if (p, l) == (rp, rl):
        return 0
    return max(1, abs(_CIDX[l] - _CIDX[rp]) + abs(_CIDX[rp] - _CIDX[rl]))



def closest(cols, rules=None):
    h, m, p, l = cols
    out = []
    for d, L, rh, rm, rp, rl in (rules or RULES):
        if (d, L) in EPISODE: continue
        if (d, L) not in BLOCK_EXEMPT and (is_blocked(d, rh, rm, rl) or is_blocked_from(d, rh, rm, rp, rl)): continue
        miss = []
        if h != rh: miss.append(f"{LABELS[0]} must be {rh} (now {h})")
        if m != rm: miss.append(f"{LABELS[1]} must be {rm} (now {m})")
        c5 = _candles_to(p, l, rp, rl)   # the 5m pair counts the closed candles it really needs, not just 1
        steps = len(miss) + c5
        if 1 <= steps <= CLOSE_MAX_STEPS:
            out.append({"direction": d, "letter": L, "steps": steps, "missing": f"{LABELS[0]} {rh} · {LABELS[1]} {rm} · {LABELS[2]} {rp} → {rl}"})
    return sorted(out, key=lambda x: x["steps"])


def aligned_colour(cols):
    h, m, _p, l = cols
    return h if (h == m == l and h in ALIGN_COLOURS) else None


# ── Binance (REST only) ──────────────────────────────────────────────────────
_cache = {}   # (interval, n) -> (fetched_at, candles)
_TTL = {TF_L: 4, TF_M: 25, TF_H: 55, CTX_TF: 55}


def get_klines(interval, n):
    """Closed candles, oldest first, or (None, reason)."""
    hit = _cache.get((interval, n))
    if hit and time.time() - hit[0] < _TTL.get(interval, 30):
        return hit[1], "ok"
    try:
        now_ms = int(time.time() * 1000)
        if interval == "10m":
            # Binance has no 10m candle: build it from two 5m candles, aligned to :00, :10, :20 ... Only fully closed ones are returned.
            r = requests.get(f"{FUTURES_BASE}/fapi/v1/klines",
                             params={"symbol": SYMBOL, "interval": "5m", "limit": min(1500, 2 * (n + 1) + 4)}, timeout=10)
            r.raise_for_status()
            five = [x for x in r.json() if x[6] < now_ms]
            by_open = {int(x[0]): x for x in five}
            closed = []
            for o in sorted(by_open):
                if o % 600000 != 0 or (o + 300000) not in by_open:
                    continue
                a, b = by_open[o], by_open[o + 300000]
                closed.append([o, a[1], str(max(float(a[2]), float(b[2]))), str(min(float(a[3]), float(b[3]))), b[4],
                               str(float(a[5]) + float(b[5])), b[6]])
            closed = closed[-(n + 1):]
            _cache[(interval, n)] = (time.time(), closed)
            return closed, "ok"
        r = requests.get(f"{FUTURES_BASE}/fapi/v1/klines",
                         params={"symbol": SYMBOL, "interval": interval, "limit": n + 1}, timeout=10)
        r.raise_for_status()
        closed = [x for x in r.json() if x[6] < now_ms]
        _cache[(interval, n)] = (time.time(), closed)
        return closed, "ok"
    except Exception as e:
        return None, f"{interval}: {e}"


def get_mark():
    try:
        r = requests.get(f"{FUTURES_BASE}/fapi/v1/premiumIndex", params={"symbol": SYMBOL}, timeout=10)
        r.raise_for_status()
        return float(r.json()["markPrice"])
    except Exception as e:
        return None


MID_LEN = int(os.environ.get("STAIRCASE_MID_LEN", "100"))   # bars in the range, same as the overlay's "Mid line range"


def midline(tf, price):
    """Midpoint of the highest high / lowest low of the last MID_LEN closed bars (plus the live price),
    like the overlay's mid line. tf = the timeframe whose bars make up the range."""
    k, _ = get_klines(tf, MID_LEN)
    if not k or len(k) < MID_LEN:
        return None
    k = k[-MID_LEN:]
    hh = max(max(float(x[2]) for x in k), price); ll = min(min(float(x[3]) for x in k), price)
    return (hh + ll) / 2


def get_context(price):
    out = {}
    k, _ = get_klines(CTX_TF, 500)
    if k and len(k) >= WPR_LEN:
        hi = [float(x[2]) for x in k]; lo = [float(x[3]) for x in k]; cl = [float(x[4]) for x in k]
        out.update({"wpr": round(wpr(hi, lo, cl, WPR_LEN), 1), "ctx_tf": CTX_TF})
    k5w, _ = get_klines(TF_L, 500)      # the low-timeframe WPR / midline (named "5m" in the saved fields; it follows STAIRCASE_TF_LOW, so a 10m tracker checks the 10m chart)
    if k5w and len(k5w) >= WPR_LEN:
        out["wpr5"] = round(wpr([float(x[2]) for x in k5w], [float(x[3]) for x in k5w], [float(x[4]) for x in k5w], WPR_LEN), 1)
    for tf, nm in (("4h", "mid4h"), ("30m", "mid30"), (TF_L, "mid5")):
        m = midline(tf, price)
        if m:
            out[nm] = round(m, 1); out[nm + "_side"] = "above" if price > m else "below"
            out[nm + "_dist"] = round((price - m) / m * 100, 2)
    return out


# ── Telegram (optional, plain text, no buttons — Orion keeps the Take buttons) ──
def tg(text):
    if not (TG_ALERTS and TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"[Telegram] {e}")


# ── Logging ─────────────────────────────────────────────────────────────────
def add_entry(kind, key, bar, price, cols, vals, ctx):
    if skipday():          # Saturday etc.: never log
        return None
    with LOCK:
        if any(e["key"] == key and e.get("bar") == bar for e in HISTORY[-60:]):
            return None
        sides = [ALIGN_DIR[key[6:]]] if kind == "align" else [key.split("_")[0]]
        obs = kind == "align" and blocked(sides[0], cols)   # blocked alignment: logged and counted, never a trade
        if obs: sides = []
        e = {"t": time.time(), "bar": bar, "kind": kind, "key": key, "price": price,
             "cols": list(cols), "vals": [round(v, 1) for v in vals], "cfg": cfg_sig(),
             "tp_pct": TP_PCT, "tp2_pct": TP2_PCT, "tp3_pct": TP3_PCT, "sl_pct": SL_PCT, "res": {s: None for s in sides}, "ended": None}
        if obs: e["obs"] = True
        e.update(ctx or {})
        if key in POCKET_KEYS:
            e["pocket"] = in_pocket(e)
        HISTORY.append(e)
        del HISTORY[:-MAX_HISTORY]
        save_history()
    return e


def evaluate(bar, price, cols, vals, ctx):
    """Called once per newly closed fast-TF candle."""
    h, m, p, l = cols
    ctx_line = (f"\nWPR {ctx['wpr']:.1f} ({ctx['ctx_tf']})" if ctx.get("wpr") is not None else "") if ctx else ""
    if ctx and ctx.get("mid30") is not None:
        ctx_line += f"\nPrice {ctx['mid30_side']} the 30m midline"
    gate = {"short": short_gated(ctx), "long": long_gated(ctx) or oversold_gated(ctx) or green30_gated(cols, ctx)}
    for d, L in detect(cols) + detect(cols, UNTESTED):
        if gate[d] and (d, L) not in GATE_EXEMPT:
            print(f"[Tracker] {d} {L} blocked by the midline/WPR filters (30m WPR {ctx.get('wpr')}, 5m WPR {ctx.get('wpr5')}, {ctx.get('mid30_side')} 30m midline)")
    for d, L in detect(cols):
        if not cond_ok(d, L, ctx):
            print(f"[Tracker] {d} {L} not fired: midline conditions not met ({(ctx or {}).get('mid30_side')} 30m, {(ctx or {}).get('mid5_side')} 5m)")
            continue
        if gate[d] and (d, L) not in GATE_EXEMPT: continue
        ent = add_entry("setup", f"{d}_{L}", bar, price, cols, vals, ctx)
        if ent:
            print(f"[Tracker] FIRE {d} {L} @ {price}" + (" (off-zone)" if offzone(ent) else ""))
            if not offzone(ent): tg(f"{'🟢' if d == 'long' else '🔴'} <b>{d.upper()} {L}</b> · BTC {price:,.0f}\n"
               f"{LABELS[0]} {EMOJI[h]} · {LABELS[1]} {EMOJI[m]} · {LABELS[2]} {EMOJI[p]}→{EMOJI[l]}\n"
               f"<i>{NOTES[(d, L)]}</i> (tracker log){ctx_line}")
    hits = set(episode_hits(cols, ctx))
    for key in list(EP_ACTIVE):
        if key not in hits:
            EP_ACTIVE.discard(key); store_set("ep_active", json.dumps(sorted(f"{a}_{b}" for a, b in EP_ACTIVE)))
    for d, L in sorted(hits):
        if (d, L) in EP_ACTIVE:
            continue
        ent = add_entry("setup", f"{d}_{L}", bar, price, cols, vals, ctx)
        if ent:
            EP_ACTIVE.add((d, L)); store_set("ep_active", json.dumps(sorted(f"{a}_{b}" for a, b in EP_ACTIVE)))
            print(f"[Tracker] FIRE {d} {L} @ {price}")
            tg(f"{'🟢' if d == 'long' else '🔴'} <b>{d.upper()} {L}</b> · BTC {price:,.0f}\n<i>{NOTES[(d, L)]}</i> (tracker log){ctx_line}")
    for d, L in detect(cols, UNTESTED):
        if (gate[d] and (d, L) not in GATE_EXEMPT) or not cond_ok(d, L, ctx): continue
        if add_entry("untested", f"{d}_{L}", bar, price, cols, vals, ctx):
            print(f"[Tracker] UNTESTED FIRE {d} {L} @ {price}")
    ac = aligned_colour(cols)      # the real alignment (decides when a stay ends)
    ac_log = ac if (ac and not gate[ALIGN_DIR[ac]] and align_cond_ok(ac, ctx)) else None   # whether it may be logged
    with LOCK:
        open_al = next((e for e in reversed(HISTORY[-200:]) if e["kind"] == "align" and e.get("ended") is None), None)
        if open_al and open_al["key"] != f"align_{ac}":
            open_al["ended"] = time.time()
            save_history()
        if ac_log and not (open_al and open_al["key"] == f"align_{ac}"):
            ent = add_entry("align", f"align_{ac}", bar, price, cols, vals, ctx)
            if ent:
                print(f"[Tracker] ALIGNED {ac} @ {price}" + (" (off-zone)" if offzone(ent) else ""))
                if not offzone(ent) and not ent.get("obs"): tg(f"{EMOJI[ac]} <b>ALL {ac.upper()}</b> · {'/'.join(LABELS)} aligned · BTC {price:,.0f}{ctx_line}")


def resolve_all():
    """Settle open logs with 1m candles after the fire.
    loss = SL first. tp1 = TP1 hit (R = TP1/SL), upgraded to tp2 if TP2 is then hit before the stop.
    After TP1 the stop moves to the entry price (set STAIRCASE_BE_AFTER_TP1=false to keep the original SL).
    A candle that touches both the stop and a target counts against you."""
    with LOCK:
        pend = [e for e in HISTORY if any(v is None for v in e["res"].values()) and e.get("price")]
    changed = False
    for e in pend:
        try:
            tp1_pct, sl_pct = float(e["tp_pct"]), float(e["sl_pct"])
            tp2_pct = float(e.get("tp2_pct", TP2_PCT))
            tp3_pct = float(e.get("tp3_pct", TP3_PCT))
            entry = float(e["price"])
            start = e.get("scan") or int((e["t"] // 60 + 1) * 60 * 1000)
            r = requests.get(f"{FUTURES_BASE}/fapi/v1/klines",
                             params={"symbol": SYMBOL, "interval": "1m", "startTime": int(start), "limit": 1500}, timeout=10)
            r.raise_for_status()
            now_ms = int(time.time() * 1000)
            last_close = None
            t1 = e.setdefault("tp1", {})
            t2 = e.setdefault("tp2", {})
            for x in r.json():
                if x[6] >= now_ms:
                    break
                hi, lo = float(x[2]), float(x[3])
                at = x[6] / 1000
                last_close = float(x[4]); e["scan"] = x[0] + 60000; changed = True
                for side, cur in e["res"].items():
                    if cur is not None:
                        continue
                    short = side == "short"
                    lvl = (lambda p: entry * (1 - p / 100)) if short else (lambda p: entry * (1 + p / 100))
                    sl = entry * (1 + sl_pct / 100) if short else entry * (1 - sl_pct / 100)
                    hit1 = lo <= lvl(tp1_pct) if short else hi >= lvl(tp1_pct)
                    hit2 = lo <= lvl(tp2_pct) if short else hi >= lvl(tp2_pct)
                    hit3 = lo <= lvl(tp3_pct) if short else hi >= lvl(tp3_pct)

                    def done(outcome, rr):
                        e["res"][side] = {"outcome": outcome, "r": rr, "at": at}
                        if outcome in ("tp1", "tp2", "tp3"):
                            e["res"][side]["t1"] = t1.get(side, at)

                    if side not in t1:
                        # how far price went against the entry before TP1, as a share of the stop distance (1.0 = the stop)
                        adv = ((hi - entry) if short else (entry - lo)) / entry * 100
                        mm = e.setdefault("mae", {})
                        mm[side] = round(max(mm.get(side, 0.0), min(max(adv, 0.0) / sl_pct, 1.0)), 2)
                        hit_sl = hi >= sl if short else lo <= sl
                        if hit_sl:
                            done("loss", -1.0)
                        elif hit1:
                            t1[side] = at
                            if hit2:
                                t2[side] = at
                    else:
                        stop = entry if BE_AFTER_TP1 else sl
                        stop_hit = hi >= stop if short else lo <= stop
                        if side in t2:
                            # TP2 already hit: runs on to TP3 (2R); falling back to the stop banks TP2
                            if stop_hit:
                                done("tp2", round(tp2_pct / sl_pct, 2))
                            elif hit3:
                                done("tp3", round(tp3_pct / sl_pct, 2))
                            elif at - e["t"] >= TP2_MAX_HOURS * 3600:
                                # 5h after entry with TP2 hit but no TP3: close at market. TP2 stays banked, so never less than the TP2 R, and capped at the TP3 R
                                mv = ((entry - last_close) if short else (last_close - entry)) / entry * 100
                                rr = round(max(tp2_pct / sl_pct, min(mv / sl_pct, tp3_pct / sl_pct)), 2)
                                done("tp2", rr)
                                e["res"][side]["timed"] = True
                        elif stop_hit:
                            done("tp1", round(tp1_pct / sl_pct, 2))
                        elif hit2:
                            t2[side] = at
                if all(v is not None for v in e["res"].values()):
                    break
            if time.time() - e["t"] > TIMEOUT_SECS:
                ref = last_close if last_close is not None else entry
                for side, cur in e["res"].items():
                    if cur is None:
                        if side in t2:
                            e["res"][side] = {"outcome": "tp2", "r": round(tp2_pct / sl_pct, 2), "at": time.time(), "t1": t1[side]}
                        elif side in t1:
                            e["res"][side] = {"outcome": "tp1", "r": round(tp1_pct / sl_pct, 2), "at": time.time(), "t1": t1[side]}
                        else:
                            mv = (entry - ref) / entry * 100 if side == "short" else (ref - entry) / entry * 100
                            e["res"][side] = {"outcome": "timeout", "r": round(mv / sl_pct, 2), "at": time.time()}
                        changed = True
        except Exception as ex:
            print(f"[Tracker] resolve error: {ex}")
    if changed:
        with LOCK:
            save_history()


# ── Weekly summary ───────────────────────────────────────────────────────────
DIGEST_ON   = os.environ.get("STAIRCASE_WEEKLY_DIGEST", "true").lower() == "true"
DIGEST_DAY  = int(os.environ.get("STAIRCASE_DIGEST_DAY", "6"))     # 0 = Monday ... 6 = Sunday (UK time)
DIGEST_HOUR = int(os.environ.get("STAIRCASE_DIGEST_HOUR", "20"))   # UK hour
_new = lambda: {"fires": 0, "n": 0, "tp1": 0, "both": 0, "sl": 0, "to": 0, "open": 0, "r": 0.0}


def _add(a, res):
    if res is None:
        a["open"] += 1
        return
    a["n"] += 1; a["r"] += float(res["r"])
    o = res["outcome"]
    if o == "tp3": a["both"] += 1; a["t3"] = a.get("t3", 0) + 1
    elif o == "tp2": a["both"] += 1
    elif o in ("tp1", "win"): a["tp1"] += 1
    elif o == "loss": a["sl"] += 1
    else: a["to"] += 1


def _label(k, last=None):
    old = k.endswith("|old")
    if old:
        k = k[:-4]
    if k.startswith("align_"):
        c = k[6:]
        return f"All {c} ({ALIGN_DIR[c]})"
    d, L = k.split("_", 1)
    return f"{d.title()} {L}" + ((f" (retired version, last used {last})" if last else " (retired version)") if old else "")


def build_digest(now=None, days=7):
    """Returns (html for Telegram, plain text) for the last `days` days on the current settings."""
    now = now or time.time(); cfg = cfg_sig(); since = now - days * 86400
    per, tot, oldlast = {}, _new(), {}
    plus, rest, offt = _new(), _new(), {"long": _new(), "short": _new()}
    unt = {}
    with LOCK:
        for e in HISTORY:
            if hidden(e) or e.get("cfg") != cfg or e["t"] < since:
                continue
            if (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT):
                continue
            if e["kind"] == "untested":
                if not offzone(e):
                    uk_ = e["key"] + ("|old" if retired_version(e) else "")
                    for side, res in e["res"].items():
                        _add(unt.setdefault(uk_, _new()), res); unt[uk_]["fires"] += 1
                continue
            if offzone(e):
                for side, res in e["res"].items():
                    _add(offt[side], res)
                continue
            pk = e["key"] + ("|old" if retired_version(e) else "")
            if pk.endswith("|old"):
                oldlast[pk] = max(oldlast.get(pk, 0.0), e["t"])
            a = per.setdefault(pk, _new()); a["fires"] += 1; tot["fires"] += 1
            for side, res in e["res"].items():
                _add(a, res); _add(tot, res)
                if e["kind"] == "setup":
                    _add(plus if e["key"].endswith("+") else rest, res)
    fmt = lambda a: f"{a['tp1']}/{a['both']}/{a['sl']}"
    avg = lambda a: f"{a['r']/a['n']:+.2f}R" if a["n"] else "-"
    uk = datetime.fromtimestamp(now, ZoneInfo("Europe/London"))
    start = datetime.fromtimestamp(since, ZoneInfo("Europe/London"))
    lines = [f"Staircase weekly{(' (' + NAME + ')') if NAME else ''} · {start.strftime('%d %b')} to {uk.strftime('%d %b')}",
             f"TP1 {TP_PCT}% / TP2 {TP2_PCT}% / TP3 {TP3_PCT}% ({TP3_R:g}R) / SL {SL_PCT}%", ""]
    if not tot["fires"]:
        lines.append("No fires this week.")
    else:
        lines.append(f"{tot['fires']} logged, {tot['n']} finished, {tot['open']} still open")
        if tot["n"]:
            lines.append(f"{tot['tp1']} TP1 only · {tot['both'] - tot.get('t3', 0)} TP2 · {tot.get('t3', 0)} TP3 · {tot['sl']} SL" + (f" · {tot['to']} timed out" if tot["to"] else ""))
            lines.append(f"Total {tot['r']:+.1f}R · average {avg(tot)}")
        lines += ["", "⭐ Reminder: your theory is that the + setups (A++, A+, B+) are the real signals, best R and best confirmation. They are still tests, so check them here:"]
        for nm, a in (("+ setups", plus), ("All other setups", rest)):
            lines.append(f"{nm}: {a['n']} finished, {fmt(a)}, {avg(a)}" if a["n"] else f"{nm}: nothing finished yet")
        lines += ["", "By trigger (TP1 only / both / SL):"]
        rows = []
        for k in sorted(per, key=lambda k: (k.startswith("align_"), k)):
            a = per[k]
            rows.append(f"{_label(k, _fmt_day(oldlast[k]) if k in oldlast else None):<44}{a['fires']:>3} fired  {fmt(a):<7} {avg(a):>7}" + (f"  +{a['open']} open" if a["open"] else ""))
        lines += ["\x00" + r for r in rows]
        done = [(k, a) for k, a in per.items() if a["n"] >= 3 and not k.endswith("|old")]
        sd = lambda k: ALIGN_DIR[k[6:]] if k.startswith("align_") else k.split("_")[0]
        lines.append("")
        for side, nm in (("long", "longs"), ("short", "shorts")):
            grp = [(k, a) for k, a in done if sd(k) == side]
            if not grp:
                lines.append(f"Best / worst {nm}: none with 3+ finished trades yet")
                continue
            best = max(grp, key=lambda t: t[1]["r"] / t[1]["n"]); worst = min(grp, key=lambda t: t[1]["r"] / t[1]["n"])
            lines.append(f"Best {nm[:-1]}: {_label(best[0])} {avg(best[1])} ({best[1]['n']} finished)")
            if worst[0] != best[0]:
                lines.append(f"Worst {nm[:-1]}: {_label(worst[0])} {avg(worst[1])} ({worst[1]['n']} finished)")
        lines.append("(only current triggers with 3+ finished trades)")
        if unt:
            lines += ["", "Untested setups (kept out of the totals above):"]
            for k in sorted(unt):
                a = unt[k]
                lines.append(f"{_label(k)}: {a['fires']} fired, {a['n']} finished, {fmt(a)}, {avg(a)}" + (f", {a['open']} open" if a["open"] else ""))
        if ZONE_FILTER and any(offt[sd]["n"] or offt[sd]["open"] for sd in offt):
            lines += ["", f"Off-zone, not counted above (longs with price above / shorts with price below the {'5m' if ZONE_RANGE == 'mid5' else '30m'} midline):"]
            for sd, nm in (("long", "Longs above the midline"), ("short", "Shorts below the midline")):
                a = offt[sd]
                if a["n"] or a["open"]:
                    lines.append(f"{nm}: {a['n']} finished, {fmt(a)}, {avg(a)}" + (f", {a['open']} open" if a["open"] else ""))
    text = "\n".join(lines)
    plain = text.replace("\x00", "")
    html_lines = []
    block = []
    for ln in lines:
        if ln.startswith("\x00"):
            block.append(ln[1:])
            continue
        if block:
            html_lines.append("<pre>" + "\n".join(block) + "</pre>"); block = []
        html_lines.append(ln)
    if block:
        html_lines.append("<pre>" + "\n".join(block) + "</pre>")
    html = "📊 <b>" + html_lines[0] + "</b>\n" + "\n".join(html_lines[1:])
    return html, plain


WEEKLY = []   # saved weekly reports, newest last: {key, t, text}


def maybe_weekly():
    """Once a week (default Sunday 20:00 UK): save the report for the page, and send it to Telegram if configured."""
    if not DIGEST_ON or not STORAGE["loaded"]:
        return
    uk = datetime.now(ZoneInfo("Europe/London"))
    if uk.weekday() != DIGEST_DAY or uk.hour < DIGEST_HOUR:
        return
    key = uk.strftime("%G-W%V")
    if store_get("digest_sent") == key:
        return
    html, plain = build_digest()
    WEEKLY.append({"key": key, "t": time.time(), "text": plain})
    store_set("weekly_reports", json.dumps(WEEKLY))
    store_set("digest_sent", key)
    print("[Tracker] weekly report saved")
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": html[:4000], "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            print(f"[Tracker] digest telegram error: {e}")


# ── Refresh + loop ───────────────────────────────────────────────────────────
def refresh():
    """Read the market, build the live snapshot. Returns (bar, price, cols, vals, ctx) or None."""
    k5, s5 = get_klines(TF_L, ZLEN + 3)
    k4, s4 = get_klines(TF_H, ZLEN)
    k30, s30 = get_klines(TF_M, ZLEN)
    mp = get_mark()
    if not (k5 and k4 and k30 and mp):
        bad = [s for s, v in ((s5, k5), (s4, k4), (s30, k30)) if not v]
        STATE["why"] = "no data: " + ("; ".join(bad) if bad else "price")
        return None
    c5 = [float(x[4]) for x in k5]
    cur5, prev5 = composite(c5), composite(c5[:-1])
    h = composite([float(x[4]) for x in k4] + ([mp] if high_live() else []))
    m = composite([float(x[4]) for x in k30] + ([mp] if mid_live() else []))
    if None in (cur5, prev5, h, m):
        STATE["why"] = "not enough candle history yet"
        return None
    cols = (colour(h), colour(m), colour(prev5), colour(cur5))
    live5 = composite(c5 + [mp])
    ctx = get_context(mp)
    ctx["comp_h"], ctx["comp_m"], ctx["comp_l"] = round(h, 2), round(m, 2), round(cur5, 2)   # live composites, used by per-setup minimums (COMP_MIN)
    now = time.time(); step = TF_SECS.get(TF_L, 300)
    STATE.update(why="", updated=now, snap={
        "price": mp, "h": h, "m": m, "l": cur5, "cols": list(cols), "live5": live5,
        "live5_col": colour(live5) if live5 is not None else None,
        "next_close": now - (now % step) + step, "tfs": list(LABELS),
        "params": [LOOKBACK, ZLEN, HIGH, LOW], "live": [high_live(), mid_live()], "firing": [] if skipday(now) else [{"direction": d, "letter": L} for d, L in detect(cols)],
        "close": [] if skipday(now) else closest(cols),
        "ufiring": [] if skipday(now) else [{"direction": d, "letter": L} for d, L in detect(cols, UNTESTED)],
        "uclose": [] if skipday(now) else closest(cols, UNTESTED), "aligned": None if skipday(now) else aligned_colour(cols), "ctx": ctx, "skip": skipday(now)})
    return k5[-1][0], mp, cols, (h, m, cur5), ctx


def loop():
    last_resolve = 0.0
    while True:
        try:
            time.sleep(5)
            if not STORAGE["loaded"] and time.time() - loop.last_load > 60:
                loop.last_load = time.time()
                load_all()
            res = refresh()
            if res:
                bar = res[0]
                if STATE["last_bar"] is None:           # first read ever: remember the bar, don't log a stale one
                    STATE["last_bar"] = bar
                    store_set("last_bar", str(bar))
                elif bar != STATE["last_bar"]:
                    STATE["last_bar"] = bar
                    store_set("last_bar", str(bar))
                    evaluate(*res)
            if time.time() - last_resolve > 60:
                last_resolve = time.time()
                resolve_all()
                check_kill_switch()
                maybe_weekly()
                if STORAGE["loaded"]:
                    touch_rules()
        except Exception as e:
            print(f"[Tracker] loop error: {e}")
            time.sleep(30)


loop.last_load = 0.0
_started = False


def load_all():
    """Load saved state. Until this succeeds nothing is written, so a database hiccup can never wipe history."""
    try:
        h = store_read("history"); sn = store_read("since"); wr = store_read("weekly_reports"); lb = store_read("last_bar")
        loaded = json.loads(h)[-MAX_HISTORY:] if h else []
        since = json.loads(sn) if sn else {}
        weekly = json.loads(wr) if wr else []
        rl = store_read("rule_log")
        rl = json.loads(rl) if rl else None
        ea = store_read("ep_active")
        ea = json.loads(ea) if ea else []
    except Exception as e:
        STORAGE["ok"] = False; STORAGE["error"] = f"could not load saved history: {e}"[:200]
        print(f"[Tracker] load error (writes paused until it loads): {e}")
        return False
    with LOCK:
        sig = lambda e: (e["key"], e.get("bar"), round(float(e["t"])))
        have = {sig(e) for e in loaded}
        extra = [e for e in HISTORY if sig(e) not in have]
        HISTORY[:] = (loaded + extra)[-MAX_HISTORY:]
        for k, v in since.items():
            SINCE.setdefault(k, v)
        if not WEEKLY:
            WEEKLY.extend(weekly)
        EP_ACTIVE.update(tuple(x.split("_", 1)) for x in ea)
        if rl and not RULE_LOG["cur"] and not RULE_LOG["retired"]:
            RULE_LOG["cur"], RULE_LOG["retired"] = rl.get("cur", {}), rl.get("retired", [])
        if lb and STATE["last_bar"] is None:
            STATE["last_bar"] = int(lb)
        # alignments count only in their own direction (Red/Orange short, Green/Cyan long): drop any other leg
        changed = bool(extra)
        for e in HISTORY:
            if e.get("key") in ("long_A", "short_A"):      # Long/Short A were renamed A++
                e["key"] += "++"; changed = True
            if e.get("key") in POCKET_KEYS and e.get("pocket") is None and e.get("wpr") is not None:
                e["pocket"] = in_pocket(e); changed = True
            if e.get("kind") == "align" and not e.get("obs"):
                want = ALIGN_DIR.get(e["key"][6:])
                if want and set(e["res"]) != {want}:
                    e["res"] = {want: e["res"].get(want)}
                    changed = True
        STORAGE["loaded"] = True; STORAGE["ok"] = True; STORAGE["error"] = ""
        if cfg_sig() not in SINCE:
            SINCE[cfg_sig()] = time.time()
            save_since()
        if changed:
            save_history()
    print(f"[Tracker] loaded {len(HISTORY)} saved entries from {STORAGE['mode']}")
    return True


def start():
    global _started
    if _started:
        return
    _started = True
    for attempt in range(3):
        if load_all():
            break
        time.sleep(2)
    if not DATABASE_URL:
        print(f"[Tracker] WARNING: no DATABASE_URL, saving to {DATA_DIR} (lost on redeploy unless it is a volume)")
    threading.Thread(target=loop, daemon=True).start()
    print(f"[Tracker] started: {'/'.join(LABELS)}, {SYMBOL}, TP1 {TP_PCT}% / TP2 {TP2_PCT}% / TP3 {TP3_PCT}% / SL {SL_PCT}%")


# ── Web ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)
_fails = []
ICON_B64 = "iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAIAAACyr5FlAAAG10lEQVR42u3dO2xkVxnA8e8759w7D8/EGTvJSpuH5QUpgiChlDQRUiSooKAEItEgihSIiCIS0CAoaCkiKKgJrwKIWNGgKCvRpUCb5RXLq2XZZSWvbewdj2fm3nM+irE3jhSByL3XO3f1/1fb7Njj+5sz51zPOdbV0QUh+qAcPwICB4GDwEHgIHAQOAgcBA4CB4GDCBwEDgIHgYPAQeAgcFDLC809tNb6aMa1eghwqIhTFZHSLJppTdfVqyqXq9U4vGppdliWyWwtyx4NIYnVclGPYirN8NFWHEH1oCyHwX/u8bUX10bPDwfreUiVx41kshLcV6/9/cr+wTCEaLzDtArH4gW9WxSfWR9959LGJwZ9VZklq+W1vsARVEHR1pFjbvbq5tOvbDwlIv8uSxHRmmYJyaQwhot24giqu0Xx6uYz3/3Ixq3ZTEV8rXNHrXvhQ+eEw6selOVn10evbDx5azZ3rCkeolzF13RpNvT+25c2VESE1QQ47v9n1aMYX1hbfW7YH8fkGTXAcXbkSCYvro2cqHEPExxnZRSWRll4fjiYpeQYNsBxtmgyDG49zyL3LsHxgTchuGUJjv/2/kLgIHAQgYPAQeAgcBA4CBwEDgIHgYPAQeAgAgeBg8BB4CBwEDioTYVWfJeLTzCriGqzH1nls9LtwxFUVaRMUpQSmzTonbD/pk04VOSwLItkqz0ddbRMjQweJqIqR1MrIj5agmNxcsvX/vzulZ3D119+5NMfC4fH5hqYKZnJSq5f/NH4zb8Uj/Q0Jmy0Z+Qok632dH2owYl3jSgcdDXzTDtaOOeQ0zlHESVZIziKEhltX61oI3OCxYPT2bjPQeAgcBA4CBwEDgIHgYPAQeAgcBCBg8BB4CBwEDgIHAQOAgeBg8BBBA4CB4GDwEHgIHDQkhf4Edzv/ibqhrbjvveFDByte6E4UZWULEZr9HwO1Xb8/W5wnF4wkcNjm5fW6+b9vm8Ux3xepJTA0Y6SyUpHX/rx0VvXpl956fOXNjems5k28Oo2szzPf/qz32xt3+h2OrbcbzDgeN/IUZyMHD3nXXM4vG/HOgAc75tzyMmcI8WYGsIRY2JC2vrVSkNzxhadH8R9DgIHgYPAQeAgcBA4CBwEDgIHgYMIHAQOAgeBg8BB4CBwEDgIHAQOInAQOAgcBA4CB4GDljz2yj6gtPlds5X3a4Pj3Mdqp6pqZTIXTRrbb2+ivupmcHCcd9PpvCxjGHZDv9fo4S1pMrcyiYKjDZ0c6/OL327/bfvZ1740+NRmvDdT18CbSzK3km9//ef3/rjtBx1LBo62jRyjvnrfFI5BrsFXfNcCx4ObcxTRyijN4LAiMiFt+WqloQOEaloKcZ+DwEHgIHAQOAgcBA4CB4GDwEHgIAIHgYPAQeAgcBA4CBwEDgIHgeP/TfG1nKlU/1x7pSurKrNkk5SUi7FcMtSKmI7mFX18eBwmkqvenRfvjI86zrXmzyw/9Jlp7ue3D463drQbqlyWaiOHaBJ7c+/AqUBjWWwkc91s/PY/yt2xC77K1qZKOKJZ3/s/7O3fns27zuFjSSYbFtP+5WvqXMVNb5VwmEjHuZvT2Q9v3B4EXyZ4POhho4h+1N9/4+rhW++64YffQl3PaiWaPZqFn9z61y/v3L3YzedMPR6ojDDqT7d2bn7/snZC9Xf6OtahJrlz39q6/sbO3hN5piKlWeJaneMM1GKymML6yvT63evf/FW5P3F5WIqN1EkkV71Xxi9f/es3Np58+emLj2XZcUqzlJKZSKXVVDJJp+ffLP6dTLTu0ensV7HTmriI7z2snT6ZyqtWzYPvZhbT3q//dPN7l8v9iR90LNbw8qxnl30SyVSD6g+u3/z93f0vXHjshdHqZq87CKHiyUbJZBBcUBWRfq55VwdRvKsfR7erwYmYZFnW6eTJTOve/25mnU6uqmLiuplb6ViySudzqFoR57cPxm/f2P/dO4dXtrQT6pIhIro6ulDj8/eqkxinKa2G8NF+7/E8K8208mNeGx/tzcpPPhNGKxqbecfyTq7+M+7ei09dfKLX66ZmJtfq9M6dncl4svLxi361J7HCsUwm4jRN5sdbO+XuWL1zg45YDefENYVjMYtxqqXZNKXqMhY/hJ5zwelkVvGMq//xVfq5Bi/zeZlS0gbu3KiImWRZcN6n47nFpNWejYmpc64TNPNiZnWDrh/HWSWqWnXScTLsm4k4bfZ0xpTERFRVpal7eipysp5z9TwVW0xfmvl2GzzZJ0mdQ9xiZnAON2KtySP+7Kz3pV8G8StVAgeBg8BB4CBwEDgIHAQOAgeBgwgcBA4CB4GDwEHgIHBQ2/sPmPiwpGNJ3gAAAAAASUVORK5CYII="


def auth():
    if not TOKEN:
        return None
    return hmac.compare_digest(flask_request.headers.get("X-Secret", ""), TOKEN)


def gate():
    """Returns an error response or None if the request is allowed."""
    now = time.time()
    _fails[:] = [t for t in _fails if now - t < 300]
    if len(_fails) >= 10:
        return jsonify({"error": "Too many attempts"}), 429
    ok = auth()
    if ok is None:
        return jsonify({"error": "STAIRCASE_TOKEN not set"}), 503
    if not ok:
        _fails.append(now)
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.route("/")
def root():
    return redirect("/staircase")


@app.route("/health")
def health():
    return jsonify({"status": "running", "updated": STATE["updated"], "entries": len(HISTORY), "storage": STORAGE})


@app.route("/staircase-icon.png")
def icon():
    resp = app.response_class(base64.b64decode(ICON_B64), mimetype="image/png")
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/staircase")
def page():
    resp = app.response_class(PAGE, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


MID_EDGES = [-1.5, -1, -0.5, -0.25, 0, 0.25, 0.5, 1, 1.5]


def dist_bucket(d, edges):
    import bisect
    i = bisect.bisect_right(edges, d)
    if i == 0:
        return f"below {edges[0]:g}%"
    if i == len(edges):
        return f"above +{edges[-1]:g}%"
    return f"{edges[i-1]:+g}% to {edges[i]:+g}%"


def dist_labels(edges):
    return [f"below {edges[0]:g}%"] + [f"{edges[i-1]:+g}% to {edges[i]:+g}%" for i in range(1, len(edges))] + [f"above +{edges[-1]:g}%"]


def _dist_stats(cfg, field, edges):
    """Results of logged trades grouped by how far price was from a reference line at fire time.
    out[side]["__above"/"__below"] = whole side of the line, plus out[side][bucket] = distance buckets."""
    out = {"long": {}, "short": {}}
    for e in HISTORY:
        d = e.get(field)
        if e["kind"] == "untested" or hidden(e) or d is None or e.get("cfg") != cfg or (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT):
            continue
        d = float(d)
        for side, res in e["res"].items():
            for b in (dist_bucket(d, edges), "__above" if d > 0 else "__below"):
                a = out[side].setdefault(b, {"n": 0, "w": 0, "t2": 0, "l": 0, "to": 0, "r": 0.0, "open": 0})
                if res is None:
                    a["open"] += 1
                else:
                    a["n"] += 1; a["r"] += float(res["r"])
                    a["w"] += 1 if res["outcome"] in ("win", "tp1", "tp2", "tp3") else 0
                    a["t2"] += 1 if res["outcome"] in ("tp2", "tp3") else 0
                    a["t3"] = a.get("t3", 0) + (1 if res["outcome"] == "tp3" else 0)
                    a["l"] += 1 if res["outcome"] == "loss" else 0
                    a["to"] += 1 if res["outcome"] == "timeout" else 0
    for side in out.values():
        for a in side.values():
            a["r"] = round(a["r"] / a["n"], 2) if a["n"] else None
    return out


WPR_ZONES = [("-100 to -95 (extreme low)", -100.01, -95), ("-95 to -76", -95, -75), ("-75 to -50", -75, -50), ("-50 to -25", -50, -25), ("-25 to -5", -25, -5), ("-5 to 0 (extreme high)", -5, 0.01)]


def _wpr_stats(cfg, field):
    """Results grouped by the WPR reading (30m or 5m, length WPR_LEN) at fire time."""
    out = {"long": {}, "short": {}}
    for e in HISTORY:
        w = e.get(field)
        if hidden(e) or w is None or e.get("cfg") != cfg or (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT):
            continue
        w = float(w)
        z = next((n for n, lo, hi in WPR_ZONES if lo <= w < hi), None)
        if z is None: continue
        for side, res in e["res"].items():
            a = out[side].setdefault(z, {"n": 0, "w": 0, "t2": 0, "l": 0, "to": 0, "r": 0.0, "open": 0})
            if res is None:
                a["open"] += 1
            else:
                a["n"] += 1; a["r"] += float(res["r"])
                a["w"] += 1 if res["outcome"] in ("win", "tp1", "tp2", "tp3") else 0
                a["t2"] += 1 if res["outcome"] in ("tp2", "tp3") else 0
                a["t3"] = a.get("t3", 0) + (1 if res["outcome"] == "tp3" else 0)
                a["l"] += 1 if res["outcome"] == "loss" else 0
                a["to"] += 1 if res["outcome"] == "timeout" else 0
    for side in out.values():
        for a in side.values():
            a["r"] = round(a["r"] / a["n"], 2) if a["n"] else None
    return out


def _pocket_stats(cfg):
    """A++ results split by whether the 30m WPR was inside the pocket when it fired."""
    out = {"long": {}, "short": {}}
    for e in HISTORY:
        if e.get("key") not in POCKET_KEYS or hidden(e) or e.get("cfg") != cfg: continue
        if (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT): continue
        pk = e.get("pocket")
        if pk is None: continue
        for side, res in e["res"].items():
            a = out[side].setdefault("in" if pk else "out", {"n": 0, "w": 0, "t2": 0, "l": 0, "to": 0, "r": 0.0, "open": 0})
            if res is None:
                a["open"] += 1
            else:
                a["n"] += 1; a["r"] += float(res["r"])
                a["w"] += 1 if res["outcome"] in ("win", "tp1", "tp2", "tp3") else 0
                a["t2"] += 1 if res["outcome"] in ("tp2", "tp3") else 0
                a["t3"] = a.get("t3", 0) + (1 if res["outcome"] == "tp3" else 0)
                a["l"] += 1 if res["outcome"] == "loss" else 0
                a["to"] += 1 if res["outcome"] == "timeout" else 0
    for side in out.values():
        for a in side.values():
            a["r"] = round(a["r"] / a["n"], 2) if a["n"] else None
    return out


def _stats(cfg, off=False, old=False):
    """stats[key][side] = {n, w, r, open}; plus _short / _long over every trigger (setups + alignments, each counted separately), and _short_setup / _short_align / _long_setup / _long_align."""
    stats = {}

    def bump(k, side, res, t0=0):
        s = stats.setdefault(k, {}).setdefault(side, {"n": 0, "w": 0, "t2": 0, "l": 0, "to": 0, "r": 0.0, "open": 0})
        if res is None:
            s["open"] += 1
        else:
            s["n"] += 1; s["r"] += float(res["r"]); s["w"] += 1 if res["outcome"] in ("win", "tp1", "tp2", "tp3") else 0
            s["t2"] += 1 if res["outcome"] in ("tp2", "tp3") else 0
            s["t3"] = s.get("t3", 0) + (1 if res["outcome"] == "tp3" else 0)
            s["l"] += 1 if res["outcome"] == "loss" else 0
            s["to"] += 1 if res["outcome"] == "timeout" else 0
            if res.get("t1"):
                s["tt"] = s.get("tt", 0) + max(0, res["t1"] - t0); s["tn"] = s.get("tn", 0) + 1

    for e in HISTORY:
        same = (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) == (TP_PCT, SL_PCT, TP2_PCT)
        if hidden(e) or e.get("cfg") != cfg or offzone(e) != off or same == old:
            continue
        for side, res in e["res"].items():
            if not retired_version(e):
                bump(e["key"], side, res, e["t"])
            if e["kind"] == "untested":
                continue                      # untested setups never feed the totals
            bump(f"_{side}", side, res, e["t"])                     # every trigger counts, even when two fire on the same candle
            bump(f"_{side}_{'setup' if e['kind'] == 'setup' else 'align'}", side, res, e["t"])
    for sides in stats.values():
        for s in sides.values():
            s["r"] = round(s["r"] / s["n"], 2) if s["n"] else None
    return stats


def _settings_history():
    """Distinct TP1/TP2/SL combos other than the current one that logs were made under, with dates and what changed."""
    g = {}
    for e in HISTORY:
        if hidden(e): continue
        k = (float(e["tp_pct"]), float(e.get("tp2_pct", TP2_PCT)), float(e["sl_pct"]))
        if k == (TP_PCT, TP2_PCT, SL_PCT): continue
        a = g.setdefault(k, {"n": 0, "from": e["t"], "to": e["t"]})
        a["n"] += 1; a["from"] = min(a["from"], e["t"]); a["to"] = max(a["to"], e["t"])
    out = []
    for (tp1, tp2, sl), a in sorted(g.items(), key=lambda kv: kv[1]["from"]):
        diff = [f"{nm} {old:g} → {new:g}" for nm, old, new in (("TP1", tp1, TP_PCT), ("TP2", tp2, TP2_PCT), ("SL", sl, SL_PCT)) if old != new]
        out.append({"tp1": tp1, "tp2": tp2, "sl": sl, "n": a["n"], "from": a["from"], "to": a["to"], "diff": ", ".join(diff)})
    return out


def _finished(cfg):
    """Yield (entry, res) for every finished trade on the current settings: real setups and alignments, in-zone, current rules."""
    for e in HISTORY:
        if hidden(e) or e.get("cfg") != cfg or e.get("kind") == "untested" or offzone(e) or retired_version(e):
            continue
        if (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT):
            continue
        for side, res in e["res"].items():
            if res is not None:
                yield e, res


def setup_totals(cfg=None):
    """Total R and finished count per setup key, for the real setups (current settings, in-zone)."""
    cfg = cfg or cfg_sig(); out = {}
    for e, res in _finished(cfg):
        if e.get("kind") != "setup":
            continue
        a = out.setdefault(e["key"], {"n": 0, "r": 0.0}); a["n"] += 1; a["r"] += float(res["r"])
    return out


PAUSED = set()        # latched: once a setup is paused it stays paused until you resume it (Insights tab)
KILL_BASE = {}        # setup key -> its total R at the moment you resumed it; it pauses again when it falls -7R below that
_kill_loaded = False


def _load_kill():
    global _kill_loaded
    if _kill_loaded or not STORAGE["loaded"]:
        return
    try:
        raw = store_get("kill_paused"); base = store_get("kill_base")
        with LOCK:
            PAUSED.update(json.loads(raw) if raw else [])
            KILL_BASE.update(json.loads(base) if base else {})
        _kill_loaded = True
    except Exception as ex:
        print(f"[Tracker] kill state load error: {ex}")


def _save_kill():
    store_set("kill_paused", json.dumps(sorted(PAUSED)))
    store_set("kill_base", json.dumps(KILL_BASE))


def paused_keys():
    with LOCK:
        return sorted(PAUSED)


def check_kill_switch():
    """Latches a setup as paused the first time its total reaches -7R (measured from its resume point, if it was resumed).
    It stays paused until you resume it on the Insights tab. The bot reads the same list to stop alerting."""
    try:
        _load_kill()
        if not _kill_loaded:
            return
        with LOCK:
            tot = setup_totals()
            new = [k for k, a in tot.items() if k not in PAUSED and a["r"] - KILL_BASE.get(k, 0.0) <= -KILL_R]
            PAUSED.update(new)
        if new:
            _save_kill()
            for k in sorted(new):
                tg(f"⏸ <b>{_label(k)} paused</b>\nIts total hit -{KILL_R:g}R. The bot stops alerting it until you resume it on the Insights tab. The tracker keeps scoring it.")
    except Exception as ex:
        print(f"[Tracker] kill switch error: {ex}")


@app.route("/api/staircase/resume", methods=["POST"])
def api_resume():
    bad = gate()
    if bad:
        return bad
    key = str((flask_request.get_json(silent=True) or {}).get("key", ""))
    with LOCK:
        if key not in PAUSED:
            return jsonify({"ok": False, "error": "not paused"}), 404
        PAUSED.discard(key)
        KILL_BASE[key] = setup_totals().get(key, {"r": 0.0})["r"]
    _save_kill()
    tg(f"▶️ <b>{_label(key)} resumed</b>\nYou turned it back on. It pauses again if it falls another {KILL_R:g}R from here.")
    return jsonify({"ok": True})


def insights(cfg):
    """Time-of-day / weekday breakdown, paused setups, and promote / retire suggestions."""
    uk = ZoneInfo("Europe/London")
    hours = [{"label": f"{h:02d}:00-{h + 4:02d}:00" if h + 4 < 24 else f"{h:02d}:00-24:00", "n": 0, "r": 0.0, "w": 0} for h in range(0, 24, 4)]
    days = [{"label": n, "n": 0, "r": 0.0, "w": 0} for n in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")]
    for e, res in _finished(cfg):
        dt = datetime.fromtimestamp(e["t"], uk)
        for b in (hours[dt.hour // 4], days[dt.weekday()]):
            b["n"] += 1; b["r"] += float(res["r"]); b["w"] += 1 if res["outcome"] in ("win", "tp1", "tp2", "tp3") else 0
    for b in hours + days:
        b["r"] = round(b["r"], 2); b["avg"] = round(b["r"] / b["n"], 2) if b["n"] else None
    heat = {}
    for e, res in _finished(cfg):
        if res["outcome"] not in ("tp1", "tp2", "tp3", "win"):
            continue
        sd = next((sd for sd, r_ in e["res"].items() if r_ is res), None)
        v = (e.get("mae") or {}).get(sd)
        if v is not None:
            heat.setdefault(e["key"], []).append(float(v))
    heat_rows = []
    for k, v in heat.items():
        v.sort()
        heat_rows.append({"key": k, "n": len(v), "avg": round(sum(v) / len(v), 2), "med": round(v[len(v) // 2], 2),
                          "deep": round(sum(1 for x in v if x >= 0.5) / len(v) * 100)})
    heat_rows.sort(key=lambda r: r["avg"])
    tot = setup_totals(cfg)
    paused = [{"key": k, "r": round(tot.get(k, {"r": 0.0})["r"], 2), "n": tot.get(k, {"n": 0})["n"]} for k in paused_keys()]
    retire, promote = [], []
    for k, a in tot.items():
        if a["n"] >= INS_MIN_N and a["r"] / a["n"] <= RETIRE_AVG_R:
            retire.append({"key": k, "n": a["n"], "r": round(a["r"], 2), "avg": round(a["r"] / a["n"], 2)})
    unt = {}
    for e in HISTORY:
        if hidden(e) or e.get("cfg") != cfg or e.get("kind") != "untested" or offzone(e) or retired_version(e):
            continue
        if (float(e["tp_pct"]), float(e["sl_pct"]), float(e.get("tp2_pct", TP2_PCT))) != (TP_PCT, SL_PCT, TP2_PCT):
            continue
        for side, res in e["res"].items():
            if res is not None:
                a = unt.setdefault(e["key"], {"n": 0, "r": 0.0}); a["n"] += 1; a["r"] += float(res["r"])
    for k, a in unt.items():
        if a["n"] >= INS_MIN_N and a["r"] / a["n"] >= PROMOTE_AVG_R:
            promote.append({"key": k, "n": a["n"], "r": round(a["r"], 2), "avg": round(a["r"] / a["n"], 2)})
    # untested ones still collecting data: show progress so you know how far from a verdict
    waiting = [{"key": k, "n": a["n"], "r": round(a["r"], 2), "avg": round(a["r"] / a["n"], 2) if a["n"] else None} for k, a in sorted(unt.items()) if a["n"] < INS_MIN_N]
    return {"hours": hours, "days": days, "heat": heat_rows, "paused": paused, "retire": retire, "promote": promote, "waiting": waiting,
            "kill_r": KILL_R, "min_n": INS_MIN_N, "promote_avg": PROMOTE_AVG_R, "retire_avg": RETIRE_AVG_R,
            "totals": {k: {"n": a["n"], "r": round(a["r"], 2)} for k, a in tot.items()}}


@app.route("/api/staircase/paused")
def api_paused():
    bad = gate()
    if bad:
        return bad
    return jsonify({"paused": paused_keys(), "kill_r": KILL_R})


def setup_filters(d, L, h, m):
    """Plain-English list of every rule a setup has to pass before it fires, so nothing is hidden in code."""
    lo = LABELS[2]
    out = []
    if (d, L) in EPISODE:
        out.append("Fires once per stretch, not on every candle")
    out.append("Skips the blocked-colours list" if (d, L) in BLOCK_EXEMPT else "Blocked-colours list applies (Blocked tab)")
    need = SETUP_CONDS.get((d, L))
    if need:
        names = {"mid4h_side": LABELS[0], "mid30_side": LABELS[1], "mid5_side": lo}
        sides = {v for v in need.values()}
        for side in sorted(sides):
            tfs = [names[k] for k, v in need.items() if v == side]
            nm = tfs[0] if len(tfs) == 1 else ", ".join(tfs[:-1]) + " and " + tfs[-1]
            out.append(f"Price must be {side} the {nm} midline" + ("s" if len(tfs) > 1 else ""))
    for k, v in (COMP_MIN.get((d, L)) or {}).items():
        out.append(f"{lo} composite above {v:g}")
    for k, v in (COMP_CAP.get((d, L)) or {}).items():
        out.append(f"{lo} composite no higher than {v:g}")
    for k, v in (COMP_FLOOR.get((d, L)) or {}).items():
        out.append(f"{lo} composite at least {v:g}")
    for k, v in (COMP_MAX.get((d, L)) or {}).items():
        out.append(f"{lo} composite below {v:g}")
    lim = WPR5_LIMIT.get((d, L))
    if lim:
        out.append(f"{lo} WPR {lim[0]} {lim[1]:g}")
    if (d, L) in GATE_EXEMPT:
        out.append("Skips the general WPR / midline filters")
    elif d == "short":
        out.append(f"General filter: no short when price is below the {LABELS[1]} midline and the {lo} WPR is over {SHORT_GATE_WPR:g}")
    else:
        out.append(f"General filter: no long when price is above the {LABELS[1]} midline and the {lo} WPR is under {LONG_GATE_WPR:g}")
        out.append(f"Oversold filter: no long when the {LABELS[1]} WPR is under {OVERSOLD_WPR:g} and the {lo} WPR is still at or below it")
        if m == "Green":
            out.append(f"Green {LABELS[1]} filter: no long when price is below the {LABELS[1]} midline and the {lo} WPR is outside {GREEN_BAND[0]:g} to {GREEN_BAND[1]:g}")
    return out


def global_filters():
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    out = [f"Not logged or alerted on {', '.join(days[i] for i in sorted(SKIP_DAYS))} (UK time)"] if SKIP_DAYS else []
    if DROP_MID_GREY:
        out.append(f"Any setup that fires with a Grey {LABELS[1]} is hidden")
    out.append(f"{LABELS[1]} colour is read live only in the last {MID_LIVE_LAST_MIN:g} minutes of its candle, {LABELS[0]} only in the last {HIGH_LIVE_LAST_MIN:g}; otherwise the last closed candle")
    return out


def _t5(txt):
    """On a tracker whose low timeframe isn't 5m (e.g. a 10m test) the rule texts say that timeframe instead of '5m'."""
    if LABELS[2] == "5m" or not isinstance(txt, str):
        return txt
    for a in ("5m WPR", "5m composite", "5m midline", "5m range"):
        txt = txt.replace(a, a.replace("5m", LABELS[2]))
    return txt


@app.route("/api/staircase")
def api():
    bad = gate()
    if bad:
        return bad
    now = time.time(); cfg = cfg_sig(); week = now - 7 * 86400
    with LOCK:
        counts, durs, counts_off = {}, {}, {}
        for e in HISTORY:
            if e.get("cfg") != cfg or hidden(e):
                continue
            if e["t"] >= week and not retired_version(e):
                tgt = counts_off if offzone(e) else counts
                tgt[e["key"]] = tgt.get(e["key"], 0) + 1
            if e["kind"] == "align" and e.get("ended"):
                durs.setdefault(e["key"], []).append(e["ended"] - e["t"])
        recent = [dict(e, old=(e.get("cfg") != cfg), off=offzone(e), retired=retired_version(e), retired_last=(last_used(e['key'], e.get('cols', [])) if retired_version(e) else None)) for e in [x for x in HISTORY[-800:] if not hidden(x)][-300:][::-1]]
        stats = _stats(cfg)
        stats_off = _stats(cfg, off=True)
        stats_old = _stats(cfg, old=True)
        ins = insights(cfg)
    avg_dur = {k: round(sum(v) / len(v) / 60) for k, v in durs.items()}
    resp = jsonify({"snapshot": STATE["snap"], "why": STATE["why"], "updated": STATE["updated"],
                    "name": NAME, "global_filters": global_filters(), "insights": ins, "history": recent, "counts": counts, "counts_off": counts_off, "stats": stats, "stats_off": stats_off, "stats_old": stats_old, "settings_hist": _settings_history(), "zone_filter": [ZONE_FILTER, ZONE_RANGE], "leverage": LEVERAGE, "avg_dur": avg_dur, "mid_stats": {"30m": _dist_stats(cfg, "mid30_dist", MID_EDGES), "5m": _dist_stats(cfg, "mid5_dist", MID_EDGES)},
                    "mid_labels": dist_labels(MID_EDGES), "wpr_stats": {"30m": _wpr_stats(cfg, "wpr"), "5m": _wpr_stats(cfg, "wpr5")}, "wpr_labels": [z[0] for z in WPR_ZONES], "blocked_if": [{"side": b[0], "cond": _t5(b[1]), "why": _t5(b[2]), "date": b[3]} for b in BLOCKED_IF], "blocked": [{"side": b[0], "h": b[1], "m": b[2], "l": b[3], "why": b[4], "date": b[5]} for b in BLOCKED] + [{"side": b[0], "h": b[1], "m": b[2], "l": f"{b[4]} (after {b[3]})", "why": b[5], "date": b[6]} for b in BLOCKED_FROM], "pocket_stats": _pocket_stats(cfg), "pocket": POCKET, "mid_len": MID_LEN, "storage": dict(STORAGE, entries=len(HISTORY)),
                    "weekly": {"now": build_digest()[1], "reports": list(reversed(WEEKLY[-12:]))},
                    "tpsl": [TP_PCT, SL_PCT, TP2_PCT, TP3_PCT], "since": SINCE.get(cfg), "now": now, "tfs": list(LABELS),
                    "align_colours": ALIGN_COLOURS, "align_dir": ALIGN_DIR,
                    "untested": [{"direction": d, "letter": L, "h": rh, "m": rm, "p": rp, "l": rl, "note": _t5(UNOTES[(d, L)]), "filters": setup_filters(d, L, rh, rm)}
                                 for (d, L, rh, rm, rp, rl) in sorted(UNTESTED, key=lambda r: (r[0] != "short", int("".join(c for c in r[1] if c.isdigit()) or 0)))],
                    "rules": [{"direction": d, "letter": L, "h": rh, "m": rm, "p": rp, "l": rl, "note": _t5(NOTES[(d, L)]), "filters": setup_filters(d, L, rh, rm)}
                              for (d, L, rh, rm, rp, rl) in RULES]})
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/staircase/digest")
def api_digest():
    bad = gate()
    if bad:
        return bad
    _, plain = build_digest()
    return jsonify({"text": plain})


@app.route("/api/staircase/clear", methods=["POST"])
def clear():
    bad = gate()
    if bad:
        return bad
    with LOCK:
        HISTORY.clear()
        SINCE[cfg_sig()] = time.time()
        save_history(); save_since()
    print("[Tracker] history cleared from the page")
    return jsonify({"ok": True})


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Staircase">
<link rel="apple-touch-icon" href="/staircase-icon.png">
<title>Staircase</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#0f1117;color:#d8dbe2;font:15px -apple-system,Segoe UI,Roboto,sans-serif;padding:calc(env(safe-area-inset-top,0px) + 16px) 16px 32px;max-width:520px;margin:0 auto}
.top{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:14px}
.top b{font-size:17px}.mut{color:#8b90a0;font-size:12px}
.card{background:#171a23;border:1px solid #262b38;border-radius:12px;padding:14px;margin-bottom:14px}
.row{display:grid;grid-template-columns:42px 1fr;gap:8px;align-items:center;margin-bottom:8px}
.row:last-child{margin-bottom:0}
.lbl{color:#8b90a0;font-size:12px}
.chip{display:flex;justify-content:space-between;align-items:center;border-radius:8px;padding:11px 12px;font-weight:600;color:#1a1a19;font-size:15px}
.chip small{font-weight:500;font-size:11px;opacity:.75}
.pill{font-weight:600;font-size:12px;border-radius:5px;padding:2px 7px;color:#1a1a19;outline:1px solid rgba(26,26,25,.4)}
h2{font-size:13px;color:#8b90a0;font-weight:500;margin:0 0 8px}
.b{display:inline-block;border-radius:5px;padding:2px 8px;font-size:12px;font-weight:600;color:#1a1a19;margin-right:8px}
.line{display:flex;align-items:center;margin-bottom:7px;font-size:13px}
.line:last-child{margin-bottom:0}
.fire{border-color:#3dd68c}
.tabs{display:flex;gap:8px;margin-bottom:12px}
.tabs button{flex:1;padding:9px 2px;border-radius:8px;border:1px solid #262b38;background:#171a23;color:#8b90a0;font-size:13px;font-weight:600}
.tabs{gap:5px}
pre.rep{white-space:pre-wrap;font:12px/1.5 ui-monospace,Menlo,monospace;color:#d8dbe2;margin:0;overflow-x:auto}
.tabs button.on{background:#222836;color:#d8dbe2;border-color:#3a4256}
.g3{display:grid;grid-template-columns:1fr 1fr 2fr;gap:5px}
.lbl2{color:#8b90a0;font-size:11px;text-align:center}
.pc{display:block;text-align:center;padding:8px 0;border-radius:6px;color:#1a1a19;font-weight:600;font-size:13px}
.five{display:grid;grid-template-columns:1fr 14px 1fr;align-items:center;gap:2px}
.five i{text-align:center;color:#8b90a0;font-style:normal}
.tg{font-size:11px;padding:2px 6px;border-radius:4px;color:#1a1a19;font-weight:600}
.empty{color:#8b90a0;font-size:13px}
input{width:100%;padding:12px;border-radius:8px;border:1px solid #333a4a;background:#0f1117;color:#d8dbe2;font-size:16px;margin-bottom:10px}
button{width:100%;padding:12px;border-radius:8px;border:0;background:#2a78d6;color:#fff;font-size:15px;font-weight:600}
a.out{color:#8b90a0;font-size:12px;text-decoration:underline;cursor:pointer}
</style></head><body>
<div id="login" class="card" style="display:none">
 <h2>Password</h2>
 <input id="pw" type="password" autocomplete="current-password" placeholder="Staircase password">
 <button id="go">Unlock</button>
 <div class="mut" id="err" style="margin-top:8px"></div>
</div>
<div id="app" style="display:none">
<div class="top"><b>BTC</b><span class="mut" id="meta">connecting…</span></div>
<div class="tabs"><button class="on" data-t="live">Live</button><button data-t="setups">Setups</button><button data-t="align">Aligned</button><button data-t="untested">Untested</button><button data-t="mid">Midline</button><button data-t="wpr">WPR</button><button data-t="insights">Insights</button><button data-t="blocked">Blocked</button><button data-t="weekly">Weekly</button></div>
<div id="tab_live">
<div class="card" id="chips"><div class="empty">Loading…</div></div>
<div class="card" id="lastfired"></div>
<div class="card" id="state"></div>
<div class="card"><h2>Last 7 days</h2><div id="counts"></div><div class="mut" id="since"></div></div>
<div class="card"><h2>Results (all time, current settings)</h2><div id="results"></div></div>
<div class="card"><h2>Recent</h2><div id="hist"><div class="empty">Nothing yet</div></div></div>
</div>
<div id="tab_setups" style="display:none"></div>
<div id="tab_align" style="display:none"></div>
<div id="tab_untested" style="display:none"></div>
<div id="tab_mid" style="display:none"></div>
<div id="tab_wpr" style="display:none"></div>
<div id="tab_insights" style="display:none"></div>
<div id="tab_blocked" style="display:none"></div>
<div id="tab_weekly" style="display:none"></div>
<div class="mut" id="params" style="text-align:center;margin-bottom:8px"></div>
<div class="mut" id="store" style="text-align:center;margin-bottom:8px"></div>
<div style="text-align:center"><a class="out" id="out">Lock this device</a> &nbsp;·&nbsp; <a class="out" id="clr">Clear history</a></div>
</div>
<script>
const KEY='stair_secret';
const CH={Red:'#e24b4a',Orange:'#ef9f27',Grey:'#888780',Green:'#3dd68c',Cyan:'#22d3ee'};
const SC={'short_A++':'#e24b4a',short_B:'#ef9f27',short_C:'#ffeb3b',short_D:'#b794f4',short_E:'#f78fb3','long_A++':'#22d3ee',long_B:'#3dd68c',long_C:'#4c8dff',long_D:'#8e6bff',long_E:'#5eead4',long_F:'#2dd4bf',short_G:'#fda4af',short_H:'#fdba74',long_G:'#5eead4',long_H:'#2dd4bf','long_A+':'#e2e8f0','short_A+':'#e2e8f0','long_B+':'#cbd5e1','short_B+':'#cbd5e1',long_U1:'#d8b4fe',long_U2:'#f0abfc',long_U3:'#c4b5fd',long_U4:'#e9d5ff',short_U1:'#d8b4fe',short_U2:'#f0abfc',short_F:'#e879f9',short_J:'#facc15',short_K:'#38bdf8',short_L:'#fb923c',short_M:'#f472b6',long_J:'#a3e635',long_K:'#67e8f9',long_L:'#fcd34d',long_M:'#2dd4bf',short_U8:'#d8b4fe',short_U9:'#fbcfe8',short_U12:'#e9d5ff',long_U10:'#ddd6fe',long_U13:'#f5d0fe'};
const keyBadge=(e)=>{const k=e.key;if(k.startsWith('align_')){const c=k.slice(6);return `<span class="b" style="background:${CH[c]}">All ${c}</span>`;}const [d,L]=k.split('_');return `<span class="b" style="background:${SC[k]||(d==='short'?'#fca5a5':'#86efac')}">${d==='short'?'Short':'Long'} ${L}</span>${e.retired?` <span class="mut">(retired version${e.retired_last?', last used '+e.retired_last:''})</span>`:''}`;};
const badge=(d,L)=>`<span class="b" style="background:${SC[d+'_'+L]||(d==='short'?'#fca5a5':'#86efac')}">${d==='short'?'Short':'Long'} ${L}</span>`;
const HTF_ORDER=['Red','Orange','Grey','Green','Cyan'];
const htfSort=l=>l.slice().sort((a,b)=>HTF_ORDER.indexOf(a.h)-HTF_ORDER.indexOf(b.h));
const htfHead=(c,t0,pre)=>`<div class="line" style="margin:12px 0 6px;gap:6px"><span class="mut">${pre||''}${t0}</span><span class="pc" style="background:${CH[c]}">${c}</span></div>`;
const uk=t=>new Date(t*1000).toLocaleString('en-GB',{timeZone:'Europe/London',weekday:'short',day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'});
const $=id=>document.getElementById(id);
const sg=x=>x>0?'+'+x:''+x;
let timer=null;
function tick(){const el=document.getElementById('cd');if(!el||!window._nc)return;const left=Math.max(0,Math.round(window._nc-(Date.now()/1000+window._off)));el.textContent=(window._lbl||'5m')+' closes in '+Math.floor(left/60)+':'+String(left%60).padStart(2,'0');}
setInterval(tick,1000);
function showLogin(msg){clearInterval(timer);$('app').style.display='none';$('login').style.display='block';$('err').textContent=msg||'';}
function setTab(t){
 ['live','setups','align','untested','mid','wpr','insights','blocked','weekly'].forEach(n=>$('tab_'+n).style.display=n===t?'block':'none');
 document.querySelectorAll('.tabs button').forEach(b=>b.classList.toggle('on',b.dataset.t===t));
}
const dur=sec=>{sec=Math.max(0,sec);const m=Math.round(sec/60);if(sec<60)return '<1m';if(m<90)return m+'m';return Math.floor(m/60)+'h '+(m%60)+'m';};
const cnt=x=>`${x.n} done: ${x.w-(x.t2||0)} TP1 only · ${(x.t2||0)-(x.t3||0)} TP2 · ${x.t3||0} TP3 · ${x.l||0} SL`+(x.to?` · ${x.to} timed out`:'')+(x.tn?` · avg ${dur(x.tt/x.tn)} to TP1`:'');
const resTxt=(x,label)=>{if(!x||(!x.n&&!x.open))return '';return `<div class="mut" style="margin-top:6px">${label?label+': ':''}${x.n?`${cnt(x)} · avg ${sg(x.r)}R`:'none done yet'}${x.open?` · ${x.open} open`:''}</div>`;};
function renderSetups(d){
 const rules=d.rules||[],counts=d.counts||{},s=d.snapshot,T=d.tfs||['4H','30m','5m'],st=d.stats||{},so=d.stats_off||{},co=d.counts_off||{};
 const firing=new Set(s?s.firing.map(f=>f.direction+'_'+f.letter):[]);
 const near=new Map(s?s.close.map(c=>[c.direction+'_'+c.letter,c.steps]):[]);
 const chip=c=>`<span class="pc" style="background:${CH[c]}">${c}</span>`;
 const sec=(dir,title)=>`<h2 style="margin:4px 0 8px">${title}</h2>`+htfSort(rules.filter(r=>r.direction===dir)).map((r,i,arr)=>{
  const k=r.direction+'_'+r.letter,n=counts[k]||0;
  const tag=firing.has(k)?'<span class="tg" style="background:#3dd68c">firing</span>':near.has(k)?'<span class="tg" style="background:#ef9f27">'+near.get(k)+' step'+(near.get(k)>1?'s':'')+' away</span>':'';
  return ((i===0||arr[i-1].h!==r.h)?htfHead(r.h,T[0]):'')+`<div class="card"><div class="line" style="justify-content:space-between;margin-bottom:10px"><span style="display:flex;align-items:center">${badge(r.direction,r.letter)}<span class="mut">${r.letter.endsWith('+')?'⭐ ':''}${r.note}</span></span><span style="display:flex;align-items:center;gap:6px">${tag}<span class="mut">7d: ${n}${co[k]?' · +'+co[k]+' off-zone':''}</span></span></div><div class="g3"><span class="lbl2">${T[0]}</span><span class="lbl2">${T[1]}</span><span class="lbl2">${T[2]} (before → now)</span>${chip(r.h)}${chip(r.m)}<span class="five">${chip(r.p)}<i>→</i>${chip(r.l)}</span></div><div class="mut" style="margin-top:6px;font-size:12px;line-height:1.5">${(r.filters||[]).map(f=>'• '+f).join('<br>')}</div>${(resTxt((st[k]||{})[r.direction])||'<div class="mut" style="margin-top:6px">No results yet</div>')}${resTxt((so[k]||{})[r.direction],r.direction==='long'?'Off-zone (above midline)':'Off-zone (below midline)')}${resTxt(((d.stats_old||{})[k]||{})[r.direction],'Earlier settings (see Setups tab)')}</div>`;
 }).join('');
 $('tab_setups').innerHTML=((d.global_filters||[]).length?`<div class="card"><h2>Applies to every setup</h2><div class="mut" style="font-size:12px;line-height:1.5">${d.global_filters.map(f=>'• '+f).join('<br>')}</div></div>`:'')+'<div class="card" style="border-color:#ef9f27"><div style="font-size:13px"><b>⭐ Reminder:</b> you think the <b>+ setups (A++, A+, B+)</b> are the real signals, with the best R and the best confirmation. They are still tests, so compare their results with the others before trusting them.</div></div>'+((d.settings_hist||[]).length?`<div class="card"><h2>Settings changes</h2><div class="mut" style="margin-bottom:6px">Now: TP1 ${d.tpsl[0]}% · TP2 ${d.tpsl[2]}% · SL ${d.tpsl[1]}%</div>`+(d.settings_hist||[]).map(h=>`<div style="margin-bottom:6px"><b style="font-size:13px">${h.diff}</b><div class="mut">Was TP1 ${h.tp1}% · TP2 ${h.tp2}% · SL ${h.sl}% · ${h.n} logs, ${new Date(h.from*1000).toLocaleDateString('en-GB',{timeZone:'Europe/London',day:'numeric',month:'short'})} to ${new Date(h.to*1000).toLocaleDateString('en-GB',{timeZone:'Europe/London',day:'numeric',month:'short'})}</div></div>`).join('')+'</div>':'')+sec('short','Shorts')+sec('long','Longs');
}
function renderAlign(d){
 const s=d.snapshot,T=d.tfs||['4H','30m','5m'],st=d.stats||{},so=d.stats_off||{},co=d.counts_off||{},counts=d.counts||{},dur=d.avg_dur||{};
 const now=s?s.aligned:null;
 $('tab_align').innerHTML=`<div class="mut" style="margin-bottom:10px">Same colour on ${T.join(' / ')}. Red and Orange count as shorts, Green and Cyan as longs (TP1 ${d.tpsl[0]}% / TP2 ${d.tpsl[2]}% / TP3 ${d.tpsl[3]}% / SL ${d.tpsl[1]}%).</div>`+(d.align_colours||[]).map(c=>{
  const k='align_'+c,x=st[k]||{};
  return `<div class="card"><div class="line" style="justify-content:space-between;margin-bottom:6px"><span class="b" style="background:${CH[c]};margin:0">All ${c}</span><span style="display:flex;align-items:center;gap:6px">${now===c?'<span class="tg" style="background:#3dd68c">aligned now</span>':''}<span class="mut">7d: ${counts[k]||0}${co[k]?' · +'+co[k]+' off-zone':''}${dur[k]!==undefined?' · avg stay '+dur[k]+'m':''}</span></span></div>${(()=>{const dr=(d.align_dir||{})[c];return (resTxt(x[dr],dr==='long'?'Long trade':'Short trade'))||`<div class="mut">${dr==='long'?'Long':'Short'} trade: no results yet</div>`;})()}${(()=>{const dr=(d.align_dir||{})[c];return resTxt((so[k]||{})[dr],dr==='long'?'Off-zone (above midline)':'Off-zone (below midline)');})()}${(()=>{const dr=(d.align_dir||{})[c];return resTxt(((d.stats_old||{})[k]||{})[dr],'Earlier settings (see Setups tab)');})()}</div>`;
 }).join('');
}
function renderUntested(d){
 const rules=d.untested||[],counts=d.counts||{},s=d.snapshot,T=d.tfs||['4H','30m','5m'],st=d.stats||{},so=d.stats_off||{},co=d.counts_off||{};
 const firing=new Set(s&&s.ufiring?s.ufiring.map(f=>f.direction+'_'+f.letter):[]);
 const near=new Map(s&&s.uclose?s.uclose.map(c=>[c.direction+'_'+c.letter,c.steps]):[]);
 const chip=c=>`<span class="pc" style="background:${CH[c]}">${c}</span>`;
 $('tab_untested').innerHTML='<div class="card" style="border-color:#d8b4fe"><div style="font-size:13px"><b>Untested setups.</b> The ones you think are the best. They are logged and scored like the others (same TP / SL), but kept out of the main Results and totals until the numbers prove it.</div></div>'+(rules.length?rules.slice().sort((a,b)=>(a.direction===b.direction?0:a.direction==='short'?-1:1)||HTF_ORDER.indexOf(a.h)-HTF_ORDER.indexOf(b.h)).map((r,i,arr)=>{
  const k=r.direction+'_'+r.letter,n=counts[k]||0;
  const tag=firing.has(k)?'<span class="tg" style="background:#3dd68c">firing</span>':near.has(k)?'<span class="tg" style="background:#ef9f27">'+near.get(k)+' step'+(near.get(k)>1?'s':'')+' away</span>':'';
  return ((i===0||arr[i-1].h!==r.h||arr[i-1].direction!==r.direction)?htfHead(r.h,T[0],r.direction==='short'?'Shorts · ':'Longs · '):'')+`<div class="card"><div class="line" style="justify-content:space-between;margin-bottom:10px"><span style="display:flex;align-items:center">${badge(r.direction,r.letter)}<span class="mut">${r.note}</span></span><span style="display:flex;align-items:center;gap:6px">${tag}<span class="mut">7d: ${n}${co[k]?' · +'+co[k]+' off-zone':''}</span></span></div><div class="g3"><span class="lbl2">${T[0]}</span><span class="lbl2">${T[1]}</span><span class="lbl2">${T[2]} (before → now)</span>${chip(r.h)}${chip(r.m)}<span class="five">${chip(r.p)}<i>→</i>${chip(r.l)}</span></div><div class="mut" style="margin-top:6px;font-size:12px;line-height:1.5">${(r.filters||[]).map(f=>'• '+f).join('<br>')}</div>${(resTxt((st[k]||{})[r.direction])||'<div class="mut" style="margin-top:6px">No results yet</div>')}${resTxt((so[k]||{})[r.direction],r.direction==='long'?'Off-zone (above midline)':'Off-zone (below midline)')}${resTxt(((d.stats_old||{})[k]||{})[r.direction],'Earlier settings (see Setups tab)')}</div>`;
 }).join(''):'<div class="empty">No untested setups yet</div>');
}
function renderMid(d){
 window._d=d;const tf=window._midTf||'30m',L=d.mid_labels||[],ms=(d.mid_stats||{})[tf]||{};
 const row=(t,x)=>`<div style="margin-bottom:9px"><div class="line" style="margin-bottom:2px"><b style="font-size:13px">${t}</b></div><div class="mut">${x&&x.n?cnt(x)+' · avg '+sg(x.r)+'R':'none done yet'}${x&&x.open?' · '+x.open+' open':''}</div></div>`;
 const sec=(side,title)=>{
  const top=row('Price above the midline',(ms[side]||{}).__above)+row('Price below the midline',(ms[side]||{}).__below);
  return `<div class="card"><h2>${title}</h2>${top}</div>`;};
 const btn=t=>`<button onclick="window._midTf='${t}';renderMid(window._d)" style="margin-right:6px;padding:5px 12px;border-radius:8px;border:1px solid #2a2e3d;background:${t===tf?'#2a2e3d':'transparent'};color:#e6e8ef;font-size:13px">${t==='5m'?((d.tfs||[])[2]||'5m'):t} range</button>`;
 $('tab_mid').innerHTML=`<div class="mut" style="margin-bottom:8px">Results of logged trades by where price was against the midline (halfway between the highest high and lowest low of the last ${d.mid_len||100} bars) when they fired. 30m range = ${d.mid_len||100} thirty-minute bars. 5m range = what a 5m chart draws. Needs a few weeks of data before it means much.</div><div style="margin-bottom:10px">${btn('30m')}${btn('5m')}</div>`+sec('long','Long trades')+sec('short','Short trades');
}
function renderWpr(d){
 window._d=d;const tf=window._wprTf||'30m',L=d.wpr_labels||[],ws=(d.wpr_stats||{})[tf]||{};
 const row=(t,x)=>`<div style="margin-bottom:9px"><div class="line" style="margin-bottom:2px"><b style="font-size:13px">${t}</b></div><div class="mut">${cnt(x)+' · avg '+sg(x.r)+'R'}${x.open?' · '+x.open+' open':''}</div></div>`;
 const sec=(side,title)=>{const rows=L.filter(b=>ws[side]&&ws[side][b]&&(ws[side][b].n||ws[side][b].open)).map(b=>row(b,ws[side][b])).join('');return `<div class="card"><h2>${title}</h2>${rows||'<div class="empty">No logged trades yet</div>'}</div>`;};
 const pk=d.pocket_stats||{},pr=d.pocket||{};
 const pc=['long','short'].map(sd=>{const x=pk[sd]||{};if(!(x.in||x.out))return '';const ln=(t,y)=>y?`<div class="mut" style="margin-bottom:4px">${t}: ${y.n?cnt(y)+' · avg '+sg(y.r)+'R':'none done yet'}${y.open?' · '+y.open+' open':''}</div>`:`<div class="mut" style="margin-bottom:4px">${t}: none yet</div>`;return `<div class="card"><h2>${sd==='long'?'Long':'Short'} A++ · 30m WPR pocket ${pr[sd]?pr[sd][0]+' to '+pr[sd][1]:''}</h2>${ln('Inside the pocket',x.in)}${ln('Outside the pocket',x.out)}</div>`}).join('');
 const btn=t=>`<button onclick="window._wprTf='${t}';renderWpr(window._d)" style="margin-right:6px;padding:5px 12px;border-radius:8px;border:1px solid #2a2e3d;background:${t===tf?'#2a2e3d':'transparent'};color:#e6e8ef;font-size:13px">${t==='5m'?((d.tfs||[])[2]||'5m'):t} WPR</button>`;
 $('tab_wpr').innerHTML=`<div class="mut" style="margin-bottom:8px">Results of logged trades by the Williams %R reading when they fired (length 200). -100 is oversold, 0 is overbought. 5m WPR is only recorded on new logs, so it starts empty.</div>${pc}<div style="margin-bottom:10px">${btn('30m')}${btn('5m')}</div>`+sec('long','Long trades')+sec('short','Short trades');
}
async function resumeSetup(k){if(!confirm('Turn '+k.replace('_',' ')+' back on? It will pause again if it falls another 7R from here.'))return;try{await fetch('/api/staircase/resume',{method:'POST',headers:{'X-Secret':localStorage.getItem(KEY)||'','Content-Type':'application/json'},body:JSON.stringify({key:k})});}catch(e){}load();}
function renderInsights(d){
 const I=d.insights;if(!I){$('tab_insights').innerHTML='';return;}
 const nm=k=>keyBadge({key:k});
 const col=v=>v==null?'#8b90a0':v>0?'#3dd68c':v<0?'#e24b4a':'#8b90a0';
 const tbl=(title,rows)=>`<div class="card"><h2>${title}</h2>`+rows.map(b=>`<div class="line" style="margin-bottom:5px"><b style="min-width:92px;display:inline-block;font-size:13px">${b.label}</b><span class="mut">${b.n} done${b.n?` · ${Math.round(b.w/b.n*100)}% TP1+`:''}</span><span style="margin-left:auto;font-weight:600;color:${col(b.avg)}">${b.n?sg(b.r)+'R · avg '+sg(b.avg)+'R':'-'}</span></div>`).join('')+'</div>';
 const lst=(title,note,rows,empty)=>`<div class="card"><h2>${title}</h2><div class="mut" style="margin-bottom:8px">${note}</div>`+(rows.length?rows.map(x=>`<div class="line" style="margin-bottom:6px">${nm(x.key)}<span class="mut">${x.n} done</span><span style="margin-left:auto;font-weight:600;color:${col(x.r)}">${sg(x.r)}R${x.avg!=null?' · avg '+sg(x.avg)+'R':''}</span></div>`).join(''):`<div class="empty">${empty}</div>`)+'</div>';
 $('tab_insights').innerHTML=
  `<div class="card"><h2>Paused setups</h2><div class="mut" style="margin-bottom:8px">A setup is paused when its total R on the current settings reaches -${I.kill_r}R (not consecutive losses). The bot stops alerting it and it stays paused until you resume it here. The tracker keeps scoring it.</div>`+(I.paused.length?I.paused.map(x=>`<div class="line" style="margin-bottom:6px">${nm(x.key)}<span class="mut">${x.n} done</span><span style="margin-left:auto;font-weight:600;color:${col(x.r)}">${sg(x.r)}R</span><button onclick="resumeSetup('${x.key}')" style="margin-left:10px;padding:4px 10px;border-radius:8px;border:1px solid #2a2e3d;background:#2a2e3d;color:#e6e8ef;font-size:12px">Resume</button></div>`).join(''):'<div class="empty">None paused.</div>')+'</div>'+
  `<div class="card"><h2>Entry timing: heat before TP1</h2><div class="mut" style="margin-bottom:8px">For trades that reached TP1: how far price went against the entry first, as a share of the way to the stop (0% = never went against you, 100% = touched the stop). Low is a well-timed entry. Only trades logged since this was added are counted. Sorted best first.</div>`+((I.heat||[]).length?I.heat.map(x=>`<div class="line" style="margin-bottom:6px">${nm(x.key)}<span class="mut">${x.n} winners</span><span style="margin-left:auto;font-weight:600">avg ${Math.round(x.avg*100)}% · median ${Math.round(x.med*100)}% · ${x.deep}% went past halfway</span></div>`).join(''):'<div class="empty">No data yet. It fills as new trades reach TP1.</div>')+'</div>'+
  tbl('By time of day (UK time, when it fired)',I.hours)+tbl('By weekday',I.days)+
  lst('Consider promoting',`Untested setups with at least ${I.min_n} finished trades and an average of ${sg(I.promote_avg)}R or better.`,I.promote,'Nothing qualifies yet.')+
  lst('Consider retiring',`Main setups with at least ${I.min_n} finished trades and an average of ${sg(I.retire_avg)}R or worse.`,I.retire,'Nothing qualifies yet.')+
  lst('Untested, still collecting',`Fewer than ${I.min_n} finished trades, so no verdict yet.`,I.waiting,'None.');
}
function renderBlocked(d){
 const L=d.blocked||[];
 const BI=d.blocked_if||[];
 const rows=['short','long'].map(sd=>{const r=L.filter(b=>b.side===sd),ri=BI.filter(b=>b.side===sd);return `<div class="card"><h2>${sd==='short'?'Shorts':'Longs'} blocked (${r.length+ri.length})</h2>`+ri.map(b=>`<div style="margin-bottom:9px"><div class="line" style="margin-bottom:2px"><b style="font-size:13px">If ${b.cond}</b><span class="mut" style="margin-left:8px">${b.date}</span></div><div class="mut">${b.why}</div></div>`).join('')+(r.map(b=>`<div style="margin-bottom:9px"><div class="line" style="margin-bottom:2px"><b style="font-size:13px">${[b.h,b.m,b.l].map(x=>x==='*'?'any':x).join(' / ')}</b><span class="mut" style="margin-left:8px">${b.date}</span></div><div class="mut">${b.why}</div></div>`).join('')||'<div class="empty">None</div>')+'</div>';}).join('');
 $('tab_blocked').innerHTML=`<div class="mut" style="margin-bottom:8px">Every state where one side is blocked. Format: ${(d.tfs||['4H','30m','5m']).join(' / ')} (5m = colour now). Nothing in these states is traded, alerted or counted.</div>`+rows;
}
function renderWeekly(d){
 const w=d.weekly||{},esc=t=>String(t).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/\b(Longs?)\b/g,'<b style="color:#3dd68c">$1</b>').replace(/\b(Shorts?)\b/g,'<b style="color:#e24b4a">$1</b>').replace(/(\+\d+(?:\.\d+)?R)/g,'<span style="color:#3dd68c;font-weight:600">$1</span>').replace(/(-\d+(?:\.\d+)?R)/g,'<span style="color:#e24b4a;font-weight:600">$1</span>').replace(/^(By trigger|By price|Best|Worst|Off-zone|Staircase weekly)/gm,'<b>$1</b>');
 const past=(w.reports||[]).map(r=>`<div class="card"><h2>Report · ${uk(r.t)}</h2><pre class="rep">${esc(r.text)}</pre></div>`).join('');
 $('tab_weekly').innerHTML=`<div class="card"><h2>Last 7 days (live)</h2><pre class="rep">${esc(w.now||'')}</pre></div>`+(past?`<h2 style="margin:4px 0 8px">Past weekly reports</h2>`+past:'<div class="mut" style="margin-bottom:10px">A report is saved here every Sunday at 8pm UK time.</div>');
}
function renderStore(d){
 const st=d.storage||{},el=$('store');let msg,col='#8b90a0';
 if(st.mode==='file'){msg='⚠ Saving to a file, not a database. History is lost on every redeploy unless DATABASE_URL is set on Railway.';col='#e24b4a';}
 else if(!st.loaded){msg='⚠ Saved history not loaded yet, saving is paused. '+(st.error||'');col='#e24b4a';}
 else if(!st.ok){msg='⚠ Last save failed: '+(st.error||'unknown error');col='#e24b4a';}
 else msg='History saved to the database · '+st.entries+' entries';
 el.textContent=msg;el.style.color=col;
}
function setHf(k){window._hf=(window._hf===k)?null:k;if(window._d)render(window._d);}
function render(d){
 window._d=d;
 if(d.name){document.title=d.name+' · Staircase';const tb=document.querySelector('.top b');if(tb)tb.textContent='BTC · '+d.name;}
 renderStore(d);
 const s=d.snapshot;const T=d.tfs||['4H','30m','5m'];
 if(!s){$('chips').innerHTML='<div class="empty">Waiting for the data feed…'+(d.why?'<br><small>'+d.why+'</small>':'')+'</div>';$('state').innerHTML='';$('lastfired').innerHTML='';}
 else{
  $('params').textContent='Timeframes '+T.join(' / ')+' · lookback '+s.params[0]+' · z-score '+s.params[1]+' · extremes '+s.params[2]+'/'+s.params[3]+' · '+T[0]+' '+(s.live[0]?'live':'closed')+', '+T[1]+' '+(s.live[1]?'live':'closed');
  $('meta').textContent=s.price.toLocaleString('en-GB',{maximumFractionDigits:1})+' · '+uk(d.now)+(s.skip?' · not logging today':'');
  const r=(l,v,c,extra,pill)=>`<div class="row"><span class="lbl">${l}</span><span class="chip" style="background:${CH[c]}"><span>${c}${extra||''}</span><span style="display:flex;align-items:center;gap:8px">${pill||''}${v.toFixed(1)}</span></span></div>`;
  const live=(s.live5!==null&&s.live5!==undefined)?`<span class="pill" style="background:${CH[s.live5_col]}">live ${s.live5.toFixed(1)}</span>`:'';
  $('chips').innerHTML=r(T[0],s.h,s.cols[0])+r(T[1],s.m,s.cols[1])+r(T[2],s.l,s.cols[3],s.cols[2]!==s.cols[3]?` <small>from ${s.cols[2]}</small>`:'',live)+'<div class="mut" style="display:flex;justify-content:space-between;margin-top:10px"><span>Logs use the closed value</span><span id="cd"></span></div>';
  window._lbl=T[2];window._nc=s.next_close;window._off=d.now-Date.now()/1000;tick();
  const c=s.ctx||{};
  const sgn=v=>(v>0?'+':'')+v;
  {const lf=(d.history||[]).find(e=>e.kind!=='align'&&!e.old);
   const ago=lf?Math.max(0,Math.floor((d.now-lf.t)/60)):0;
   const agoTxt=ago<60?ago+'m ago':(Math.floor(ago/60)+'h '+(ago%60)+'m ago');
   $('lastfired').innerHTML='<h2 style="margin-bottom:12px">Last setup fired</h2>'+(lf?`<div class="line" style="margin-top:4px">${keyBadge(lf)}<span>${uk(lf.t)} · ${Number(lf.price).toLocaleString('en-GB',{maximumFractionDigits:1})}<br><small class="mut" style="display:block;margin-top:8px">${agoTxt}</small></span></div>`:'<div class="empty">Nothing fired yet</div>');}
  const st=$('state');
  const al=s.aligned?`<div class="line"><span class="b" style="background:${CH[s.aligned]}">All ${s.aligned}</span><span>aligned on every timeframe</span></div>`:'';
  if(s.firing.length){
   st.className='card fire';
   st.innerHTML='<h2>Firing on the last '+T[2]+' close</h2>'+s.firing.map(f=>`<div class="line">${badge(f.direction,f.letter)}<span>all conditions met</span></div>`).join('')+al;
  }else{
   st.className='card'+(s.aligned?' fire':'');
   st.innerHTML='<h2>Setups close to firing</h2>'+al+(s.close.length?s.close.map(c=>`<div class="line">${badge(c.direction,c.letter)}<span><small class="mut">${c.steps} step${c.steps>1?'s':''} away</small><br>${c.missing}</span></div>`).join(''):'<div class="empty">None close right now. Nothing is one step away.</div>');
  }
 }
 const letters=dir=>(d.rules||[]).filter(r=>r.direction===dir).map(r=>r.letter);
 const cell=(kk,bg,txt,n)=>`<div onclick="setHf('${kk}')" style="display:flex;align-items:center;gap:4px;cursor:pointer;opacity:${n?1:.4};border-radius:8px;padding:1px 2px;${window._hf===kk?'outline:2px solid #e6e8ef;outline-offset:1px;':''}"><span class="b" style="background:${bg};margin:0">${txt}</span><b style="font-size:15px">${n}</b></div>`;
 $('counts').innerHTML=['short','long'].map(dir=>`<div class="mut" style="font-weight:600;margin:6px 0 2px">${dir==='short'?'Shorts':'Longs'}</div>`+HTF_ORDER.map(c=>({c,rs:(d.rules||[]).filter(r=>r.direction===dir&&r.h===c)})).filter(g=>g.rs.length).map(({c,rs})=>`<div style="display:flex;align-items:center;flex-wrap:wrap;gap:4px 10px;margin:3px 0"><span style="display:flex;align-items:center;gap:4px;min-width:62px"><span class="mut" style="font-size:11px">${(d.tfs||['4H'])[0]}</span><span class="pc" style="background:${CH[c]};padding:1px 7px;font-size:11px;line-height:1.5">${c}</span></span>${rs.map(r=>cell(dir+'_'+r.letter,SC[dir+'_'+r.letter]||(dir==='short'?'#fca5a5':'#86efac'),(dir==='short'?'S':'L')+' '+r.letter,(d.counts||{})[dir+'_'+r.letter]||0)).join('')}</div>`).join('')).join('')
  +`<div class="mut" style="font-weight:600;margin:8px 0 2px">All three the same</div><div style="display:flex;flex-wrap:wrap;gap:4px 10px">${(d.align_colours||[]).map(c=>cell('align_'+c,CH[c],c,(d.counts||{})['align_'+c]||0)).join('')}</div>`;
 const tp=d.tpsl||[0.45,0.33],stt=d.stats||{};
 const ov=['short','long'].map(dir=>{const g=k=>(stt[k]||{})[dir],x=g('_'+dir),a=g('_'+dir+'_setup'),b=g('_'+dir+'_align'),nm=dir==='short'?'Shorts':'Longs';const xo=(d.stats_off||{})['_'+dir];const xoo=xo&&xo[dir];const offLine=xoo&&(xoo.n||xoo.open)?`<div class="mut" style="margin-top:8px">${dir==='short'?'Shorts below the midline (off-zone, kept separate)':'Longs above the midline (off-zone, kept separate)'}: ${xoo.n?cnt(xoo)+' · avg '+sg(xoo.r)+'R':'none done yet'}${xoo.open?' · '+xoo.open+' open':''}</div>`:'';if(!(x&&x.n))return offLine||null;const sub=(t,y)=>y&&y.n?`<div class="mut" style="margin-top:2px;padding-left:10px">${t}: ${cnt(y)} · avg ${sg(y.r)}R</div>`:'';return `<b>${nm}, every trigger (setups + alignments): ${cnt(x)} · avg ${sg(x.r)}R</b>`+sub('Setups only',a)+offLine}).filter(Boolean);
 const hfSum=window._hf?(()=>{const hf=window._hf,sd=hf.startsWith('align_')?(d.align_dir||{})[hf.slice(6)]:hf.split('_')[0];
  const line=(t,y)=>y&&(y.n||y.open)?`<div class="mut" style="margin-top:6px">${t}: ${y.n?cnt(y)+' · avg '+sg(y.r)+'R · total '+sg(Math.round(y.r*y.n*100)/100)+'R':'none done yet'}${y.open?' · '+y.open+' open':''}</div>`:'';
  const cur=((d.stats||{})[hf]||{})[sd],off=((d.stats_off||{})[hf]||{})[sd],old=((d.stats_old||{})[hf]||{})[sd];
  const body=line('Current settings',cur)+line('Off-zone (kept separate)',off)+line('Earlier settings',old);
  const tfn=d.tfs||['4H','30m','5m'],pill=c=>`<span class="pill" style="background:${CH[c]}">${c}</span>`;
  const [hd,hL]=hf.split('_'),rl=hf.startsWith('align_')?null:[...(d.rules||[]),...(d.untested||[])].find(r=>r.direction===hd&&r.letter===hL);
  const scheme=hf.startsWith('align_')?`<div class="line" style="margin-top:8px;flex-wrap:wrap;gap:6px">${tfn.join(' · ')} all ${pill(hf.slice(6))}</div>`
    :rl?`<div class="line" style="margin-top:8px;flex-wrap:wrap;gap:6px">${tfn[0]} ${pill(rl.h)} · ${tfn[1]} ${pill(rl.m)} · ${tfn[2]} ${rl.letter==='F'?pill(rl.l):pill(rl.p)+' → '+pill(rl.l)}</div>`:'';
  return `<div class="mut">TP1 ${tp[0]}% / TP2 ${tp[2]}% / TP3 ${tp[3]}% / SL ${tp[1]}%</div><div class="line" style="margin-top:8px">${keyBadge({key:hf})}<b style="margin-left:6px">only</b></div>${scheme}`+(body||'<div class="empty" style="margin-top:6px">No results yet</div>');})():null;
 $('results').innerHTML=hfSum||`<div class="mut">TP1 ${tp[0]}% / TP2 ${tp[2]}% / TP3 ${tp[3]}% / SL ${tp[1]}% (TP1 ${(tp[0]/tp[1]).toFixed(2)}R, TP2 ${(tp[2]/tp[1]).toFixed(2)}R, TP3 ${(tp[3]/tp[1]).toFixed(1)}R)</div>`+(ov.length?ov.map(t=>`<div class="line" style="margin-top:6px">${t}</div>`).join(''):'<div class="empty" style="margin-top:6px">No finished trades yet</div>');
 $('since').textContent=d.since?'Tracking since '+uk(d.since)+' on these settings':'';
 const LV=d.leverage||50;
 const f1=v=>v.toLocaleString('en-GB',{minimumFractionDigits:1,maximumFractionDigits:1});
 const lvls=e=>Object.keys(e.res||{}).map(sd=>{const p=Number(e.price),sh=sd==='short',m=(x)=>sh?p*(1-x/100):p*(1+x/100),sl=sh?p*(1+e.sl_pct/100):p*(1-e.sl_pct/100);return `${Object.keys(e.res).length>1?(sh?'Short ':'Long ')+': ':''}entry ${f1(p)} · TP1 ${f1(m(e.tp_pct))} · TP2 ${f1(m(e.tp2_pct||e.tp_pct))} · TP3 ${f1(m(e.tp3_pct||e.tp2_pct||e.tp_pct))} · SL ${f1(sl)}<br>at ${LV}x: TP1 +${(e.tp_pct*LV).toFixed(1)}% · TP2 +${((e.tp2_pct||e.tp_pct)*LV).toFixed(1)}% · TP3 +${((e.tp3_pct||e.tp2_pct||e.tp_pct)*LV).toFixed(1)}% · SL −${(e.sl_pct*LV).toFixed(1)}% of margin`;}).join('<br>');
 const sides=e=>e.obs?'<span class="mut">Logged only · blocked, not a trade</span>':Object.entries(e.res||{}).map(([sd,r])=>{const lab=e.kind==='align'?(sd==='long'?'Long':'Short')+': ':'';if(!r)return (e.tp1&&e.tp1[sd])?`<span style="font-size:12px;font-weight:600;color:#3dd68c">${lab}${(e.tp2&&e.tp2[sd])?'TP2 hit · TP3 pending':'TP1 hit · TP2 pending'}</span>`:`<span class="mut">${lab}open…</span>`;const ok=['win','tp1','tp2','tp3'].includes(r.outcome);const col=ok?'#3dd68c':r.outcome==='loss'?'#e24b4a':'#8b90a0';const nm=r.outcome==='tp3'?'TP1 + TP2 + TP3':r.outcome==='tp2'?(r.timed?'TP1 + TP2, closed at the time limit':'TP1 + TP2'):ok?'TP1 only':r.outcome==='loss'?'SL hit':'timed out';const t1x=r.t1||(['tp1','tp2','tp3'].includes(r.outcome)&&e.tp1&&e.tp1[sd]);const tm=r.outcome==='loss'?` · SL after ${dur(r.at-e.t)}`:(t1x?` · TP1 after ${dur(t1x-e.t)}`+(r.outcome==='tp3'?`, TP3 after ${dur(r.at-e.t)}`:r.outcome==='tp2'?`, TP2 after ${dur(r.at-e.t)}`:''):'');return `<span style="font-size:12px;font-weight:600;color:${col}">${lab}${nm} ${sg(r.r)}R${tm}</span>`;}).join(' &nbsp; ');
 const hf=window._hf||null,HL=(hf?d.history.filter(e=>e.key===hf):d.history).slice(0,hf?100:25);
 $('hist').innerHTML=(hf?`<div class="mut" style="margin-bottom:10px">Showing ${keyBadge({key:hf})} only (${HL.length}) · <a class="out" onclick="setHf('${hf}')" style="cursor:pointer">show all</a></div>`:'')+(HL.length?HL.map(e=>`<div style="opacity:${e.old?.45:1};margin-bottom:10px"><div class="line" style="margin-bottom:3px">${keyBadge(e)}<span>${uk(e.t)} · ${Number(e.price).toLocaleString('en-GB',{maximumFractionDigits:1})}</span></div>${e.vals&&e.vals.length?`<div class="mut">Composite: ${T[0]} ${e.vals[0]} · ${T[1]} ${e.vals[1]} · ${T[2]} ${e.vals[2]}</div>`:''}${e.mid30!==undefined?`<div class="mut">${e.mid30_side} 30m midline${e.wpr===undefined&&e.kind==='align'?(e.ended?` · stayed aligned ${Math.round((e.ended-e.t)/60)}m`:' · still aligned'):''}</div>`:''}${e.wpr!==undefined?`<div class="mut">WPR: 30m ${e.wpr}${e.wpr5!==undefined?' · 5m '+e.wpr5:''}${e.pocket===true?' · ⭐ in A++ pocket':e.pocket===false?' · outside A++ pocket':''}${e.kind==='align'?(e.ended?` · stayed aligned ${Math.round((e.ended-e.t)/60)}m`:' · still aligned'):''}</div>`:''}<div class="mut">${lvls(e)}</div><div>${sides(e)}</div>${e.mae&&Object.keys(e.mae).length?'<div class="mut">'+Object.entries(e.mae).map(([sd,v])=>(e.kind==='align'?(sd==='long'?'Long':'Short')+': ':'')+'went '+Math.round(v*100)+'% of the way to the stop'+(e.tp1&&e.tp1[sd]?' before TP1':'')).join(' · ')+'</div>':''}${e.old?'<div class="mut">old settings, not counted</div>':''}${(Number(e.tp_pct)!==d.tpsl[0]||Number(e.sl_pct)!==d.tpsl[1]||Number(e.tp2_pct||e.tp_pct)!==d.tpsl[2])?`<div class="mut">logged at TP1 ${e.tp_pct}% · TP2 ${e.tp2_pct||e.tp_pct}% · SL ${e.sl_pct}% (not the current settings)</div>`:''}${e.off&&!e.old?'<div class="mut" style="color:#ef9f27">off-zone: counted separately, not in the main results</div>':''}</div>`).join(''):'<div class="empty">Nothing yet</div>');
 renderSetups(d);renderAlign(d);renderUntested(d);renderMid(d);renderWpr(d);renderInsights(d);renderBlocked(d);renderWeekly(d);
}
async function load(){
 const s=localStorage.getItem(KEY);
 if(!s){showLogin();return;}
 try{
  const r=await fetch('/api/staircase',{headers:{'X-Secret':s},cache:'no-store'});
  if(r.status===401){localStorage.removeItem(KEY);showLogin('Wrong password');return;}
  if(r.status===429){showLogin('Too many attempts, wait a few minutes');return;}
  if(r.status===503){showLogin('Not set up yet: add STAIRCASE_TOKEN on Railway');return;}
  if(!r.ok)throw new Error(r.status);
  $('login').style.display='none';$('app').style.display='block';
  render(await r.json());
  if(!timer)timer=setInterval(load,10000);
 }catch(e){$('meta').textContent='connection lost';}
}
$('go').onclick=()=>{const v=$('pw').value.trim();if(!v)return;localStorage.setItem(KEY,v);$('pw').value='';timer=null;load();};
$('pw').addEventListener('keydown',e=>{if(e.key==='Enter')$('go').click();});
$('clr').onclick=async()=>{if(!confirm('Clear the recorded history and restart the count?'))return;try{await fetch('/api/staircase/clear',{method:'POST',headers:{'X-Secret':localStorage.getItem(KEY)||''}});}catch(e){}load();};
$('out').onclick=()=>{localStorage.removeItem(KEY);timer=null;showLogin();};
document.querySelectorAll('.tabs button').forEach(b=>b.onclick=()=>setTab(b.dataset.t));
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&localStorage.getItem(KEY))load();});
load();
</script></body></html>"""

# Start the background loop when the module loads (gunicorn imports it once with one worker)
if os.environ.get("TRACKER_NO_AUTOSTART") != "1":
    start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
