"""
STAIRCASE TRACKER — standalone service (no trading code, no Orion dependency).

What it does, for BTC perps on Binance:
  * Reads 4H / 30m / 5m candles + mark price, works out the composite colour on each (same maths as the
    TradingView composite: lookback 100, z-score 100, extremes 90/10, Yellow=Orange, Blue=Green).
  * Logs every staircase setup (Short/Long A-E) the moment it fires.
  * Logs every full alignment: same colour on 4H, 30m and 5m (Red, Orange, Green, Cyan) and how long it lasted.
  * For every log: WPR, price vs EMA200, and the outcome against TP / SL (both directions for alignments).
  * Serves a password-protected live page for the Home Screen: /staircase

Needs: STAIRCASE_TOKEN (the page password). Use DATABASE_URL (Postgres) so history survives redeploys.
Optional: TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID + TRACKER_TELEGRAM_ALERTS=true for plain (no-button) alerts.
Run:  gunicorn staircase_tracker:app --workers 1 --threads 4 --bind 0.0.0.0:$PORT   (ONE worker only)
"""
import os, time, json, base64, hmac, threading
from datetime import datetime, timezone

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
TP_PCT        = float(os.environ.get("STAIRCASE_TP_PCT", "0.45"))
SL_PCT        = float(os.environ.get("STAIRCASE_SL_PCT", "0.30"))
CTX_TF        = os.environ.get("STAIRCASE_CTX_TF", "30m")
WPR_LEN       = int(os.environ.get("STAIRCASE_WPR_LEN", "200"))
TIMEOUT_SECS  = 24 * 3600
TOKEN         = os.environ.get("STAIRCASE_TOKEN", "") or os.environ.get("READ_API_SECRET", "")
DATABASE_URL  = os.environ.get("DATABASE_URL", "")
DATA_DIR      = os.environ.get("DATA_DIR", "./data")
TG_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT       = os.environ.get("TELEGRAM_CHAT_ID", "")
TG_ALERTS     = os.environ.get("TRACKER_TELEGRAM_ALERTS", "false").lower() == "true"
MAX_HISTORY   = 3000

TF_SECS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
           "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200}


def _lbl(tf):
    return tf[:-1] + ("H" if tf.endswith("h") else "m")


LABELS = (_lbl(TF_H), _lbl(TF_M), _lbl(TF_L))
EMOJI = {"Red": "🟥", "Orange": "🟧", "Grey": "⬜", "Green": "🟩", "Cyan": "🟦"}

# (direction, letter, slow colour, mid colour, fast colour BEFORE, fast colour NOW)
RULES = [
    ("short", "A", "Red",    "Orange", "Red",    "Orange"),
    ("short", "B", "Red",    "Orange", "Orange", "Grey"),
    ("short", "C", "Orange", "Orange", "Orange", "Grey"),
    ("short", "D", "Grey",   "Grey",   "Grey",   "Green"),
    ("short", "E", "Orange", "Grey",   "Orange", "Grey"),
    ("long",  "A", "Cyan",   "Green",  "Cyan",   "Green"),
    ("long",  "B", "Cyan",   "Green",  "Green",  "Grey"),
    ("long",  "C", "Green",  "Green",  "Green",  "Grey"),
    ("long",  "D", "Grey",   "Grey",   "Grey",   "Orange"),
    ("long",  "E", "Green",  "Grey",   "Green",  "Grey"),
]
NOTES = {
    ("short", "A"): "Early entry, best R",   ("long", "A"): "Early entry, best R",
    ("short", "B"): "Final confirmation",    ("long", "B"): "Final confirmation",
    ("short", "C"): "Orange staircase",      ("long", "C"): "Green staircase",
    ("short", "D"): "Bearish pullback (raw)", ("long", "D"): "Bullish pullback (raw)",
    ("short", "E"): f"{LABELS[1]} Grey rollover", ("long", "E"): f"{LABELS[1]} Grey bounce",
}
ALIGN_COLOURS = ["Red", "Orange", "Green", "Cyan"]   # all three timeframes the same colour (Grey excluded)


def cfg_sig():
    base = f"{LOOKBACK}/{ZLEN}/{HIGH:g}/{LOW:g}"
    return base if (TF_H, TF_M, TF_L) == ("4h", "30m", "5m") else f"{'/'.join(LABELS)}|{base}"


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


def store_get(key):
    try:
        if DATABASE_URL:
            _init_db()
            conn = _db(); cur = conn.cursor()
            cur.execute("SELECT value FROM tracker_state WHERE key=%s", (key,))
            row = cur.fetchone(); cur.close(); conn.close()
            return row[0] if row else None
        p = os.path.join(DATA_DIR, f"{key}.json")
        return open(p).read() if os.path.exists(p) else None
    except Exception as e:
        print(f"[Store] read error {key}: {e}")
        return None


def store_set(key, value):
    try:
        if DATABASE_URL:
            _init_db()
            conn = _db(); cur = conn.cursor()
            cur.execute("INSERT INTO tracker_state (key,value,updated_at) VALUES (%s,%s,NOW()) "
                        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()", (key, value))
            conn.commit(); cur.close(); conn.close()
            return
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = os.path.join(DATA_DIR, f"{key}.json.tmp")
        open(tmp, "w").write(value)
        os.replace(tmp, os.path.join(DATA_DIR, f"{key}.json"))
    except Exception as e:
        print(f"[Store] write error {key}: {e}")


# ── State ────────────────────────────────────────────────────────────────────
LOCK = threading.RLock()
HISTORY = []          # newest last
SINCE = {}            # cfg -> time tracking began
STATE = {"snap": None, "why": "starting…", "last_bar": None, "updated": 0}


def save_history():
    store_set("history", json.dumps(HISTORY[-MAX_HISTORY:]))


def save_since():
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


def ema(closes, period):
    k = 2 / (period + 1)
    e = sum(closes[:period]) / period
    for c in closes[period:]:
        e = (c - e) * k + e
    return e


def wpr(highs, lows, closes, length):
    """Orion's Williams %R (length 200, 30m): typical price (H+L+C)/3, last `length` candles. -100 (low) .. 0 (high)."""
    hh, ll = max(highs[-length:]), min(lows[-length:])
    tp = (highs[-1] + lows[-1] + closes[-1]) / 3
    return -50.0 if hh == ll else (hh - tp) / (hh - ll) * -100


def detect(cols):
    return [(d, L) for (d, L, rh, rm, rp, rl) in RULES if tuple(cols) == (rh, rm, rp, rl)]


def closest(cols):
    h, m, p, l = cols
    out = []
    for d, L, rh, rm, rp, rl in RULES:
        miss = []
        if h != rh: miss.append(f"{LABELS[0]} must be {rh} (now {h})")
        if m != rm: miss.append(f"{LABELS[1]} must be {rm} (now {m})")
        if (p, l) != (rp, rl): miss.append(f"{LABELS[2]} {rp} → {rl} (now {p} → {l})")
        if len(miss) == 1:
            out.append({"direction": d, "letter": L, "missing": miss[0]})
    return out


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
        r = requests.get(f"{FUTURES_BASE}/fapi/v1/klines",
                         params={"symbol": SYMBOL, "interval": interval, "limit": n + 1}, timeout=10)
        r.raise_for_status()
        now_ms = int(time.time() * 1000)
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


def get_context(price):
    k, _ = get_klines(CTX_TF, 500)
    if not k or len(k) < 201:
        return {}
    hi = [float(x[2]) for x in k]; lo = [float(x[3]) for x in k]; cl = [float(x[4]) for x in k]
    e = ema(cl, 200)
    return {"wpr": round(wpr(hi, lo, cl, WPR_LEN), 1), "ema200": round(e, 1),
            "ema_side": "above" if price > e else "below",
            "ema_dist": round((price - e) / e * 100, 2), "ctx_tf": CTX_TF}


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
    with LOCK:
        if any(e["key"] == key and e.get("bar") == bar for e in HISTORY[-60:]):
            return None
        sides = ["long", "short"] if kind == "align" else [key.split("_")[0]]
        e = {"t": time.time(), "bar": bar, "kind": kind, "key": key, "price": price,
             "cols": list(cols), "vals": [round(v, 1) for v in vals], "cfg": cfg_sig(),
             "tp_pct": TP_PCT, "sl_pct": SL_PCT, "res": {s: None for s in sides}, "ended": None}
        e.update(ctx or {})
        HISTORY.append(e)
        del HISTORY[:-MAX_HISTORY]
        save_history()
    return e


def evaluate(bar, price, cols, vals, ctx):
    """Called once per newly closed fast-TF candle."""
    h, m, p, l = cols
    ctx_line = f"\nWPR {ctx['wpr']:.1f} · price {ctx['ema_side']} EMA200 ({ctx['ctx_tf']}, {ctx['ema_dist']:+.2f}%)" if ctx else ""
    for d, L in detect(cols):
        if add_entry("setup", f"{d}_{L}", bar, price, cols, vals, ctx):
            print(f"[Tracker] FIRE {d} {L} @ {price}")
            tg(f"{'🟢' if d == 'long' else '🔴'} <b>{d.upper()} {L}</b> · BTC {price:,.0f}\n"
               f"{LABELS[0]} {EMOJI[h]} · {LABELS[1]} {EMOJI[m]} · {LABELS[2]} {EMOJI[p]}→{EMOJI[l]}\n"
               f"<i>{NOTES[(d, L)]}</i> (tracker log){ctx_line}")
    ac = aligned_colour(cols)
    with LOCK:
        open_al = next((e for e in reversed(HISTORY[-200:]) if e["kind"] == "align" and e.get("ended") is None), None)
        if open_al and open_al["key"] != f"align_{ac}":
            open_al["ended"] = time.time()
            save_history()
        if ac and not (open_al and open_al["key"] == f"align_{ac}"):
            if add_entry("align", f"align_{ac}", bar, price, cols, vals, ctx):
                print(f"[Tracker] ALIGNED {ac} @ {price}")
                tg(f"{EMOJI[ac]} <b>ALL {ac.upper()}</b> · {'/'.join(LABELS)} aligned · BTC {price:,.0f}{ctx_line}")


def resolve_all():
    """Settle open logs against TP/SL using 1m candles. Same-candle TP+SL counts as a loss."""
    with LOCK:
        pend = [e for e in HISTORY if any(v is None for v in e["res"].values()) and e.get("price")]
    changed = False
    for e in pend:
        try:
            tp_pct, sl_pct = float(e["tp_pct"]), float(e["sl_pct"])
            entry = float(e["price"])
            start = e.get("scan") or int((e["t"] // 60 + 1) * 60 * 1000)
            r = requests.get(f"{FUTURES_BASE}/fapi/v1/klines",
                             params={"symbol": SYMBOL, "interval": "1m", "startTime": int(start), "limit": 1500}, timeout=10)
            r.raise_for_status()
            now_ms = int(time.time() * 1000)
            last_close = None
            for x in r.json():
                if x[6] >= now_ms:
                    break
                hi, lo = float(x[2]), float(x[3])
                last_close = float(x[4]); e["scan"] = x[0] + 60000; changed = True
                for side, cur in e["res"].items():
                    if cur is not None:
                        continue
                    short = side == "short"
                    tp = entry * (1 - tp_pct / 100) if short else entry * (1 + tp_pct / 100)
                    sl = entry * (1 + sl_pct / 100) if short else entry * (1 - sl_pct / 100)
                    hit_tp = lo <= tp if short else hi >= tp
                    hit_sl = hi >= sl if short else lo <= sl
                    if hit_tp or hit_sl:
                        win = hit_tp and not hit_sl
                        e["res"][side] = {"outcome": "win" if win else "loss",
                                          "r": round(tp_pct / sl_pct, 2) if win else -1.0,
                                          "at": x[6] / 1000, "same_candle": bool(hit_tp and hit_sl)}
                if all(v is not None for v in e["res"].values()):
                    break
            if time.time() - e["t"] > TIMEOUT_SECS:
                ref = last_close if last_close is not None else entry
                for side, cur in e["res"].items():
                    if cur is None:
                        mv = (entry - ref) / entry * 100 if side == "short" else (ref - entry) / entry * 100
                        e["res"][side] = {"outcome": "timeout", "r": round(mv / sl_pct, 2), "at": time.time()}
                        changed = True
        except Exception as ex:
            print(f"[Tracker] resolve error: {ex}")
    if changed:
        with LOCK:
            save_history()


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
    h = composite([float(x[4]) for x in k4] + [mp])
    m = composite([float(x[4]) for x in k30] + [mp])
    if None in (cur5, prev5, h, m):
        STATE["why"] = "not enough candle history yet"
        return None
    cols = (colour(h), colour(m), colour(prev5), colour(cur5))
    live5 = composite(c5 + [mp])
    ctx = get_context(mp)
    now = time.time(); step = TF_SECS.get(TF_L, 300)
    STATE.update(why="", updated=now, snap={
        "price": mp, "h": h, "m": m, "l": cur5, "cols": list(cols), "live5": live5,
        "live5_col": colour(live5) if live5 is not None else None,
        "next_close": now - (now % step) + step, "tfs": list(LABELS),
        "params": [LOOKBACK, ZLEN, HIGH, LOW], "firing": [{"direction": d, "letter": L} for d, L in detect(cols)],
        "close": closest(cols), "aligned": aligned_colour(cols), "ctx": ctx})
    return k5[-1][0], mp, cols, (h, m, cur5), ctx


def loop():
    last_resolve = 0.0
    while True:
        try:
            time.sleep(5)
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
        except Exception as e:
            print(f"[Tracker] loop error: {e}")
            time.sleep(30)


_started = False


def start():
    global _started
    if _started:
        return
    _started = True
    try:
        h = store_get("history")
        if h:
            HISTORY.extend(json.loads(h)[-MAX_HISTORY:])
        s = store_get("since")
        if s:
            SINCE.update(json.loads(s))
        lb = store_get("last_bar")
        if lb:
            STATE["last_bar"] = int(lb)
    except Exception as e:
        print(f"[Tracker] load error: {e}")
    if cfg_sig() not in SINCE:
        SINCE[cfg_sig()] = time.time()
        save_since()
    if not DATABASE_URL:
        print(f"[Tracker] WARNING: no DATABASE_URL, saving to {DATA_DIR} (lost on redeploy unless it is a volume)")
    threading.Thread(target=loop, daemon=True).start()
    print(f"[Tracker] started: {'/'.join(LABELS)}, {SYMBOL}, TP {TP_PCT}% / SL {SL_PCT}%")


# ── Web ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)
_fails = []
ICON_B64 = "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"


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
    return jsonify({"status": "running", "updated": STATE["updated"], "entries": len(HISTORY)})


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


def _stats(cfg):
    """stats[key][side] = {n, w, r, open}; plus _short / _long over setups only."""
    stats = {}

    def bump(k, side, res):
        s = stats.setdefault(k, {}).setdefault(side, {"n": 0, "w": 0, "r": 0.0, "open": 0})
        if res is None:
            s["open"] += 1
        else:
            s["n"] += 1; s["r"] += float(res["r"]); s["w"] += 1 if res["outcome"] == "win" else 0

    for e in HISTORY:
        if e.get("cfg") != cfg or (float(e["tp_pct"]), float(e["sl_pct"])) != (TP_PCT, SL_PCT):
            continue
        for side, res in e["res"].items():
            bump(e["key"], side, res)
            if e["kind"] == "setup":
                bump(f"_{side}", side, res)
    for sides in stats.values():
        for s in sides.values():
            s["r"] = round(s["r"] / s["n"], 2) if s["n"] else None
    return stats


@app.route("/api/staircase")
def api():
    bad = gate()
    if bad:
        return bad
    now = time.time(); cfg = cfg_sig(); week = now - 7 * 86400
    with LOCK:
        counts, durs = {}, {}
        for e in HISTORY:
            if e.get("cfg") != cfg:
                continue
            if e["t"] >= week:
                counts[e["key"]] = counts.get(e["key"], 0) + 1
            if e["kind"] == "align" and e.get("ended"):
                durs.setdefault(e["key"], []).append(e["ended"] - e["t"])
        recent = [dict(e, old=(e.get("cfg") != cfg)) for e in reversed(HISTORY[-25:])]
        stats = _stats(cfg)
    avg_dur = {k: round(sum(v) / len(v) / 60) for k, v in durs.items()}
    resp = jsonify({"snapshot": STATE["snap"], "why": STATE["why"], "updated": STATE["updated"],
                    "history": recent, "counts": counts, "stats": stats, "avg_dur": avg_dur,
                    "tpsl": [TP_PCT, SL_PCT], "since": SINCE.get(cfg), "now": now, "tfs": list(LABELS),
                    "align_colours": ALIGN_COLOURS,
                    "rules": [{"direction": d, "letter": L, "h": rh, "m": rm, "p": rp, "l": rl, "note": NOTES[(d, L)]}
                              for (d, L, rh, rm, rp, rl) in RULES]})
    resp.headers["Cache-Control"] = "no-store"
    return resp


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
.tabs button{flex:1;padding:9px;border-radius:8px;border:1px solid #262b38;background:#171a23;color:#8b90a0;font-size:14px;font-weight:600}
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
<div class="tabs"><button class="on" data-t="live">Live</button><button data-t="setups">Setups</button><button data-t="align">Aligned</button></div>
<div id="tab_live">
<div class="card" id="chips"><div class="empty">Loading…</div></div>
<div class="card" id="state"></div>
<div class="card"><h2>Right now</h2><div id="ctx" class="mut"></div></div>
<div class="card"><h2>Last 7 days</h2><div id="counts"></div><div class="mut" id="since"></div></div>
<div class="card"><h2>Results (all time, current settings)</h2><div id="results"></div></div>
<div class="card"><h2>Recent</h2><div id="hist"><div class="empty">Nothing yet</div></div></div>
</div>
<div id="tab_setups" style="display:none"></div>
<div id="tab_align" style="display:none"></div>
<div class="mut" id="params" style="text-align:center;margin-bottom:8px"></div>
<div style="text-align:center"><a class="out" id="out">Lock this device</a> &nbsp;·&nbsp; <a class="out" id="clr">Clear history</a></div>
</div>
<script>
const KEY='stair_secret';
const CH={Red:'#e24b4a',Orange:'#ef9f27',Grey:'#888780',Green:'#3dd68c',Cyan:'#22d3ee'};
const SC={short_A:'#e24b4a',short_B:'#ef9f27',short_C:'#ffeb3b',short_D:'#b794f4',short_E:'#f78fb3',long_A:'#22d3ee',long_B:'#3dd68c',long_C:'#4c8dff',long_D:'#8e6bff',long_E:'#5eead4'};
const keyBadge=(e)=>{const k=e.key;if(k.startsWith('align_')){const c=k.slice(6);return `<span class="b" style="background:${CH[c]}">All ${c}</span>`;}const [d,L]=k.split('_');return `<span class="b" style="background:${SC[k]}">${d==='short'?'Short':'Long'} ${L}</span>`;};
const badge=(d,L)=>`<span class="b" style="background:${SC[d+'_'+L]}">${d==='short'?'Short':'Long'} ${L}</span>`;
const uk=t=>new Date(t*1000).toLocaleString('en-GB',{timeZone:'Europe/London',weekday:'short',hour:'2-digit',minute:'2-digit'});
const $=id=>document.getElementById(id);
const sg=x=>x>0?'+'+x:''+x;
let timer=null;
function tick(){const el=document.getElementById('cd');if(!el||!window._nc)return;const left=Math.max(0,Math.round(window._nc-(Date.now()/1000+window._off)));el.textContent=(window._lbl||'5m')+' closes in '+Math.floor(left/60)+':'+String(left%60).padStart(2,'0');}
setInterval(tick,1000);
function showLogin(msg){clearInterval(timer);$('app').style.display='none';$('login').style.display='block';$('err').textContent=msg||'';}
function setTab(t){
 ['live','setups','align'].forEach(n=>$('tab_'+n).style.display=n===t?'block':'none');
 document.querySelectorAll('.tabs button').forEach(b=>b.classList.toggle('on',b.dataset.t===t));
}
const resTxt=(x,label)=>{if(!x||(!x.n&&!x.open))return '';return `<div class="mut" style="margin-top:6px">${label?label+': ':''}${x.n?`win ${Math.round(x.w/x.n*100)}% · avg ${sg(x.r)}R · ${x.n} done`:'none done yet'}${x.open?` · ${x.open} open`:''}</div>`;};
function renderSetups(d){
 const rules=d.rules||[],counts=d.counts||{},s=d.snapshot,T=d.tfs||['4H','30m','5m'],st=d.stats||{};
 const firing=new Set(s?s.firing.map(f=>f.direction+'_'+f.letter):[]);
 const near=new Set(s?s.close.map(c=>c.direction+'_'+c.letter):[]);
 const chip=c=>`<span class="pc" style="background:${CH[c]}">${c}</span>`;
 const sec=(dir,title)=>`<h2 style="margin:4px 0 8px">${title}</h2>`+rules.filter(r=>r.direction===dir).map(r=>{
  const k=r.direction+'_'+r.letter,n=counts[k]||0;
  const tag=firing.has(k)?'<span class="tg" style="background:#3dd68c">firing</span>':near.has(k)?'<span class="tg" style="background:#ef9f27">1 step away</span>':'';
  return `<div class="card"><div class="line" style="justify-content:space-between;margin-bottom:10px"><span style="display:flex;align-items:center">${badge(r.direction,r.letter)}<span class="mut">${r.note}</span></span><span style="display:flex;align-items:center;gap:6px">${tag}<span class="mut">7d: ${n}</span></span></div><div class="g3"><span class="lbl2">${T[0]}</span><span class="lbl2">${T[1]}</span><span class="lbl2">${T[2]} (before → now)</span>${chip(r.h)}${chip(r.m)}<span class="five">${chip(r.p)}<i>→</i>${chip(r.l)}</span></div>${(resTxt((st[k]||{})[r.direction])||'<div class="mut" style="margin-top:6px">No results yet</div>')}</div>`;
 }).join('');
 $('tab_setups').innerHTML=sec('short','Shorts')+sec('long','Longs');
}
function renderAlign(d){
 const s=d.snapshot,T=d.tfs||['4H','30m','5m'],st=d.stats||{},counts=d.counts||{},dur=d.avg_dur||{};
 const now=s?s.aligned:null;
 $('tab_align').innerHTML=`<div class="mut" style="margin-bottom:10px">Same colour on ${T.join(' / ')}. Results show what a trade in each direction would have done (TP ${d.tpsl[0]}% / SL ${d.tpsl[1]}%).</div>`+(d.align_colours||[]).map(c=>{
  const k='align_'+c,x=st[k]||{};
  return `<div class="card"><div class="line" style="justify-content:space-between;margin-bottom:6px"><span class="b" style="background:${CH[c]};margin:0">All ${c}</span><span style="display:flex;align-items:center;gap:6px">${now===c?'<span class="tg" style="background:#3dd68c">aligned now</span>':''}<span class="mut">7d: ${counts[k]||0}${dur[k]!==undefined?' · avg stay '+dur[k]+'m':''}</span></span></div>${(resTxt(x.short,'If short')+resTxt(x.long,'If long'))||'<div class="mut">No results yet</div>'}</div>`;
 }).join('');
}
function render(d){
 const s=d.snapshot;const T=d.tfs||['4H','30m','5m'];
 if(!s){$('chips').innerHTML='<div class="empty">Waiting for the data feed…'+(d.why?'<br><small>'+d.why+'</small>':'')+'</div>';$('state').innerHTML='';$('ctx').textContent='';}
 else{
  $('params').textContent='Timeframes '+T.join(' / ')+' · lookback '+s.params[0]+' · z-score '+s.params[1]+' · extremes '+s.params[2]+'/'+s.params[3];
  $('meta').textContent=s.price.toLocaleString('en-GB',{maximumFractionDigits:1})+' · '+uk(d.now);
  const r=(l,v,c,extra,pill)=>`<div class="row"><span class="lbl">${l}</span><span class="chip" style="background:${CH[c]}"><span>${c}${extra||''}</span><span style="display:flex;align-items:center;gap:8px">${pill||''}${v.toFixed(1)}</span></span></div>`;
  const live=(s.live5!==null&&s.live5!==undefined)?`<span class="pill" style="background:${CH[s.live5_col]}">live ${s.live5.toFixed(1)}</span>`:'';
  $('chips').innerHTML=r(T[0],s.h,s.cols[0])+r(T[1],s.m,s.cols[1])+r(T[2],s.l,s.cols[3],s.cols[2]!==s.cols[3]?` <small>from ${s.cols[2]}</small>`:'',live)+'<div class="mut" style="display:flex;justify-content:space-between;margin-top:10px"><span>Logs use the closed value</span><span id="cd"></span></div>';
  window._lbl=T[2];window._nc=s.next_close;window._off=d.now-Date.now()/1000;tick();
  const c=s.ctx||{};
  $('ctx').innerHTML=c.wpr!==undefined?`WPR ${c.wpr} · price ${c.ema_side} EMA200 (${c.ctx_tf}, ${c.ema_dist>0?'+':''}${c.ema_dist}%)`:'No context yet';
  const st=$('state');
  const al=s.aligned?`<div class="line"><span class="b" style="background:${CH[s.aligned]}">All ${s.aligned}</span><span>aligned on every timeframe</span></div>`:'';
  if(s.firing.length){
   st.className='card fire';
   st.innerHTML='<h2>Firing on the last '+T[2]+' close</h2>'+s.firing.map(f=>`<div class="line">${badge(f.direction,f.letter)}<span>all conditions met</span></div>`).join('')+al;
  }else{
   st.className='card'+(s.aligned?' fire':'');
   st.innerHTML='<h2>No setup firing</h2>'+al+(s.close.length?s.close.map(c=>`<div class="line">${badge(c.direction,c.letter)}<span>${c.missing}</span></div>`).join(''):'<div class="empty">Nothing is one step away.</div>');
  }
 }
 const LET=['A','B','C','D','E'];
 $('counts').innerHTML=['short','long'].map(dir=>`<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-bottom:8px">${LET.map(x=>{const n=(d.counts||{})[dir+'_'+x]||0;return `<div style="text-align:center;opacity:${n?1:.4}"><span class="b" style="background:${SC[dir+'_'+x]};margin:0">${dir==='short'?'S':'L'} ${x}</span><div style="font-size:20px;font-weight:600;margin-top:4px">${n}</div></div>`}).join('')}</div>`).join('')
  +`<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:4px">${(d.align_colours||[]).map(c=>{const n=(d.counts||{})['align_'+c]||0;return `<div style="text-align:center;opacity:${n?1:.4}"><span class="b" style="background:${CH[c]};margin:0">${c}</span><div style="font-size:20px;font-weight:600;margin-top:4px">${n}</div></div>`}).join('')}</div><div class="mut" style="margin-top:4px">Bottom row: all three timeframes the same colour</div>`;
 const tp=d.tpsl||[0.45,0.3],stt=d.stats||{};
 const ov=['short','long'].map(dir=>{const x=(stt['_'+dir]||{})[dir];return x&&x.n?`${dir==='short'?'Shorts':'Longs'}: ${Math.round(x.w/x.n*100)}% win, ${sg(x.r)}R avg (${x.n})`:null}).filter(Boolean);
 $('results').innerHTML=`<div class="mut">TP ${tp[0]}% / SL ${tp[1]}% (${(tp[0]/tp[1]).toFixed(1)}R)</div>`+(ov.length?ov.map(t=>`<div class="line" style="margin-top:6px">${t}</div>`).join(''):'<div class="empty" style="margin-top:6px">No finished trades yet</div>');
 $('since').textContent=d.since?'Tracking since '+uk(d.since)+' on these settings':'';
 const sides=e=>Object.entries(e.res||{}).map(([sd,r])=>{const lab=e.kind==='align'?'If '+sd+': ':'';if(!r)return `<span class="mut">${lab}open…</span>`;const col=r.outcome==='win'?'#3dd68c':r.outcome==='loss'?'#e24b4a':'#8b90a0';return `<span style="font-size:12px;font-weight:600;color:${col}">${lab}${r.outcome==='win'?'TP':r.outcome==='loss'?'SL':'timeout'} ${sg(r.r)}R</span>`;}).join(' &nbsp; ');
 $('hist').innerHTML=d.history.length?d.history.map(e=>`<div style="opacity:${e.old?.45:1};margin-bottom:10px"><div class="line" style="margin-bottom:3px">${keyBadge(e)}<span>${uk(e.t)} · ${Number(e.price).toLocaleString('en-GB',{maximumFractionDigits:1})}</span></div>${e.wpr!==undefined?`<div class="mut">WPR ${e.wpr} · ${e.ema_side} EMA200 (${e.ema_dist>0?'+':''}${e.ema_dist}%)${e.kind==='align'?(e.ended?` · lasted ${Math.round((e.ended-e.t)/60)}m`:' · still aligned'):''}</div>`:''}<div>${sides(e)}</div>${e.old?'<div class="mut">old settings, not counted</div>':''}</div>`).join(''):'<div class="empty">Nothing yet</div>';
 renderSetups(d);renderAlign(d);
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
