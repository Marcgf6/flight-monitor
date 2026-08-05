#!/usr/bin/env python3
"""
Flight monitor bot — watches China Southern flights and sends WhatsApp/Telegram
alerts for every meaningful event, as close to real time as the data allows:

  ⏰  ~30 min before departure (reminder)
  🕒  DELAY / new ETA            (AeroDataBox status feed)
  🚪  GATE / TERMINAL change     (AeroDataBox status feed)
  🛑  CANCELLATION / DIVERSION   (AeroDataBox status feed)
  🛫  DEPARTED (airborne)        (FlightRadar24 live feed)
  🛬  LANDED                     (FlightRadar24 live feed)

Two independent data sources, each used for what it's good at:
  * FlightRadar24 live feed (free, no key) — position-based departure/landing.
  * AeroDataBox (free RapidAPI key) — scheduled vs. estimated times, gate,
    terminal, cancellation, diversion. If no key is set, delay alerts are simply
    skipped and departure/landing still work.

Flights come from the FLIGHTS_JSON env var (used in the cloud so personal data
stays in an encrypted secret) or from flights.json on disk (local dev).

Modes:
  python3 monitor.py --serve   # long-running loop, polls every 60s (cloud)
  python3 monitor.py --once    # a single poll cycle, then exit
  python3 monitor.py           # continuous loop (local), with a startup ping
  python3 monitor.py --status  # print state and exit
  python3 monitor.py --test    # send a test alert and exit
"""

import json
import math
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import urllib.request
import urllib.parse
import urllib.error

BASE = Path(__file__).resolve().parent
FLIGHTS_FILE = BASE / "flights.json"
STATE_FILE = BASE / "state.json"
# Written only when a --serve run completes its full window. The workflow hands
# off to a successor only if it exists, so a run that crashed on startup or
# exited immediately (all flights landed) can never start a hot chain.
FULL_TERM_FLAG = BASE / ".served-full-term"
ENV_FILE = BASE / ".env"
LOG_FILE = BASE / "monitor.log"

# --- tunables ---------------------------------------------------------------
POLL_INTERVAL_SEC = 180          # cadence for the plain continuous loop (local)
SERVE_POLL_SEC = 60              # cadence in --serve mode (cloud, ~1 minute)
# How long one --serve job stays alive. Overridable so the cloud job can run
# for hours (a long job + a frequent cron = seamless handoff, no coverage gap)
# while local runs keep the short default.
SERVE_MAX_SEC = int(os.environ.get("SERVE_MAX_SEC") or 3300)
IDLE_SLEEP_SEC = 300             # sleep when no window is open (local loop)

WINDOW_BEFORE_DEP_MIN = 45       # start position-watching this long before departure
WINDOW_AFTER_ARR_HRS = 4         # keep position-watching this long after arrival
PREDEP_REMINDER_MIN = 30         # "departs soon" reminder this long before departure
MISSING_POLLS_FOR_LANDING = 3    # consecutive "gone from feed" polls before a radar-drop can mean landed
MIN_ELAPSED_FRAC_FOR_RADAR_LAND = 0.85  # a radar-drop landing also requires >= this much of the flight elapsed
# When the airline feed is silent there is nothing to corroborate a radar
# dropout, so the bar rises rather than falls: almost the whole flight elapsed,
# and a much longer silence, before anything is inferred.
MISSING_POLLS_NO_STATUS = 10
MIN_ELAPSED_FRAC_NO_STATUS = 0.98
AIRBORNE_MIN_ALT_FT = 1000       # altitude above which we consider it truly flying

STATUS_BEFORE_DEP_HRS = 10       # start status-watching (delays/gate) this long before dep
STATUS_AFTER_ARR_HRS = 2         # keep status-watching this long after arrival
# A hand-entered itinerary can disagree with the airline's own schedule. The
# airline wins: check well ahead of the status window, cheaply, so a wrong
# departure time surfaces days early instead of at the gate.
SCHEDULE_MISMATCH_MIN = 20       # tolerate this much drift before correcting
SCHEDULE_AUDIT_LEAD_HRS = 72     # start cross-checking the schedule this long before dep
SCHEDULE_AUDIT_SEC = 21600       # ...and re-check every 6h (12 extra calls per leg, total)
LANDING_NEAR_ETA_MIN = 45        # a radar dropout only counts as "landed" within this window of ETA
STATUS_POLL_SEC = 1200           # how often to hit AeroDataBox per flight (20 min → quota-friendly)
DELAY_ALERT_MIN = 15             # only alert departure delays >= this many minutes
DELAY_REALERT_MIN = 15           # re-alert when the delay changes by >= this many minutes
ARR_DELAY_ALERT_MIN = 20         # only alert arrival delays >= this many minutes

# Proactive "how's the trip going" updates while airborne (no question needed).
# A leg that finished today is still what you want to see when you ask. Older
# than this it becomes history and only "all" shows it.
RECENT_LEG_HRS = 18
PROGRESS_FRACTIONS = [0.25, 0.50, 0.75]  # send an en-route update at these points of the flight
PROGRESS_MIN_DURATION_MIN = 180          # only send milestone updates for flights >= 3h
APPROACH_BEFORE_ARR_MIN = 45             # send an "on approach / descending" update this long before ETA

# Live arrival estimate from FlightRadar24, so the ETA tracks the aircraft
# instead of being frozen at whatever was known when it took off.
FR24_ETA_POLL_SEC = 300          # refresh the live ETA this often (an extra request, so not every poll)
ETA_SHIFT_ALERT_MIN = 15         # tell the user when the arrival time moves by at least this much

# Third-party status provider. Host is configurable because a marketplace
# listing moving hosts must not require a code change to recover from.
ADB_HOST = os.environ.get("AERODATABOX_HOST", "aerodatabox.p.rapidapi.com")
PROVIDER_ALERT_AFTER_FAILS = 3   # consecutive failures before the operator is told
PROVIDER_BACKOFF_START_SEC = 900   # first back-off after a failure (doubles thereafter)
PROVIDER_BACKOFF_MAX_SEC = 21600   # never wait longer than 6h before retrying
# Cloudflare fronts the marketplace and rejects urllib's default signature
# outright (403 / "error code: 1010"), so identify as a normal client.
DEFAULT_USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/126.0.0.0 Safari/537.36")
# ---------------------------------------------------------------------------


def log(msg):
    line = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}  {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_env():
    if ENV_FILE.exists():
        for raw in ENV_FILE.read_text().splitlines():
            raw = raw.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            k, v = raw.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def load_flights():
    """Flights from FLIGHTS_JSON env (cloud secret) or flights.json (local)."""
    raw = os.environ.get("FLIGHTS_JSON")
    data = json.loads(raw) if raw else json.loads(FLIGHTS_FILE.read_text())
    for fl in data["flights"]:
        dep = datetime.strptime(fl["sched_departure_local"], "%Y-%m-%d %H:%M")
        arr = datetime.strptime(fl["sched_arrival_local"], "%Y-%m-%d %H:%M")
        fl["_dep_utc"] = dep.replace(tzinfo=ZoneInfo(fl["origin_tz"])).astimezone(timezone.utc)
        fl["_arr_utc"] = arr.replace(tzinfo=ZoneInfo(fl["dest_tz"])).astimezone(timezone.utc)
        fl["_config_dep_utc"] = fl["_dep_utc"]
    return data


def apply_schedule_overrides(data, st):
    """Re-apply a schedule correction previously learned from the airline feed.

    The correction lives in state, not in the itinerary, because the itinerary
    is an encrypted secret that only its owner can edit — the bot still has to
    track the real flight in the meantime, and across job handoffs.
    """
    fixes = st.get("_schedule") or {}
    for fl in data["flights"]:
        fix = fixes.get(fl["id"])
        if not fix:
            continue
        try:
            fl["_dep_utc"] = datetime.fromisoformat(fix["dep_utc"])
            fl["_arr_utc"] = datetime.fromisoformat(fix["arr_utc"])
        except Exception:
            continue
        fl["_schedule_corrected"] = True
    return data


def lookup_date(fl):
    """Origin-local date to ask the provider about — derived from the effective
    departure, so a corrected schedule also corrects which day we look up."""
    return fl["_dep_utc"].astimezone(ZoneInfo(fl["origin_tz"])).strftime("%Y-%m-%d")


def _mark_served_full_term():
    try:
        FULL_TERM_FLAG.write_text("ok\n")
    except Exception:
        pass


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_FILE)


# --- Notifications ----------------------------------------------------------
def _to_whatsapp(text):
    text = text.replace("<b>", "*").replace("</b>", "*")
    return re.sub(r"<[^>]+>", "", text)


def notify(text, verbose=False):
    """Send an alert, falling back to the secondary channel if the primary fails.

    A travel alert is worthless if it silently vanishes, so any configured
    second channel is tried before giving up (CallMeBot's free WhatsApp tier
    throttles aggressively and then accepts-but-drops messages).
    """
    channel = os.environ.get("NOTIFY_CHANNEL", "").strip().lower()
    have_wa = bool(os.environ.get("WHATSAPP_APIKEY") and os.environ.get("WHATSAPP_PHONE"))
    have_tg = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
    if not channel:
        channel = "whatsapp" if have_wa else ("telegram" if have_tg else "")
    available = {"whatsapp": have_wa, "telegram": have_tg}
    if not any(available.values()):
        log("!! No notification channel configured. Message would have been:\n" + text)
        return False

    # Primary first, then whatever else is configured.
    for ch in [channel] + [c for c in ("telegram", "whatsapp") if c != channel]:
        if not available.get(ch):
            continue
        ok = send_whatsapp(text, verbose=verbose) if ch == "whatsapp" else send_telegram(text)
        # Authoritative delivery record — independent of the caller's optimistic
        # ">> alert sent" line — so the logs always show whether an alert landed.
        if ok:
            log(f"   ✓ delivered via {ch}")
            return True
        log(f"   ✗ delivery failed via {ch}"
            + ("; trying fallback channel" if ch == channel else ""))
    log("   ✗ DELIVERY FAILED on all channels — alert was NOT received")
    return False


def send_whatsapp(text, attempts=3, verbose=False):
    phone = os.environ.get("WHATSAPP_PHONE", "").replace("+", "").replace(" ", "")
    apikey = os.environ.get("WHATSAPP_APIKEY", "").strip()
    if not phone or not apikey:
        log("!! WhatsApp not configured (WHATSAPP_PHONE / WHATSAPP_APIKEY missing). "
            "Message would have been:\n" + text)
        return False
    if verbose:
        # Masked config summary — never the full secret — to sanity-check the
        # phone/apikey pairing (CallMeBot can accept a request yet never deliver
        # if the number or key is wrong).
        log(f"   config: WHATSAPP_PHONE {len(phone)} digits ending ..{phone[-2:]} · "
            f"WHATSAPP_APIKEY {len(apikey)} chars")
    q = urllib.parse.urlencode({"phone": phone, "text": _to_whatsapp(text), "apikey": apikey})
    url = "https://api.callmebot.com/whatsapp.php?" + q
    last = ""
    for i in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=25) as r:
                body = r.read().decode(errors="replace")
            if verbose:
                log(f"   CallMeBot raw response (attempt {i}): {body.strip().replace(chr(10), ' ')[:400]}")
            if any(x in body.lower() for x in ("message queued", "message sent", "successfully")):
                return True
            # CallMeBot returns HTTP 200 with a human-readable error IN THE BODY when
            # the apikey is wrong/expired or the number hasn't authorised the bot
            # ("You need to activate the API..."). Surface the FULL body so the real
            # cause is visible instead of a silently-swallowed "unclear" response.
            last = body.strip().replace("\n", " ")
            # Retrying a throttled API only digs the hole deeper, so bail out
            # immediately and let notify() fall back to the other channel.
            if any(x in body.lower() for x in ("too many requests", "called to the api to often",
                                               "called to the api too often", "rate limit")):
                log("!! WhatsApp RATE-LIMITED by CallMeBot — not retrying. Response: " + last[:300])
                return False
            log(f"!! WhatsApp/CallMeBot did NOT confirm delivery (attempt {i}/{attempts}). "
                f"Full response: {last[:400]}")
        except Exception as e:
            last = str(e)
            log(f"!! WhatsApp send error (attempt {i}/{attempts}): {e}")
        if i < attempts:
            time.sleep(2 * i)
    log("!! WhatsApp NOT delivered after retries — last response: " + (last[:400] or "(empty)"))
    return False


def send_telegram(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log("!! Telegram not configured. Message would have been:\n" + text)
        return False
    payload = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=payload)
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode()).get("ok", False)
    except Exception as e:
        log(f"!! Telegram send failed: {e}")
        return False


# --- time helpers -----------------------------------------------------------
def local_and_home(dt_utc, local_tz, home_tz):
    loc = dt_utc.astimezone(ZoneInfo(local_tz)).strftime("%H:%M")
    home = dt_utc.astimezone(ZoneInfo(home_tz)).strftime("%H:%M")
    lname = local_tz.split("/")[-1].replace("_", " ")
    if local_tz == home_tz:
        return f"{loc} ({lname})"
    return f"{loc} {lname}  ·  {home} Madrid"


def hhmm(mins):
    mins = int(round(mins))
    h, m = divmod(abs(mins), 60)
    s = (f"{h}h " if h else "") + f"{m}m"
    return s.strip()


# --- FlightRadar24 live feed (departure / landing) --------------------------
def norm_num(s):
    return (s or "").upper().replace(" ", "")


def find_live_flight(api, fl):
    want_num = norm_num(fl["number"])
    want_cs = norm_num(fl["callsign"])
    flights = []
    for attempt in range(3):
        try:
            flights = api.get_flights(airline=fl["airline_icao"])
            if flights:
                break
        except Exception as e:
            log(f"   feed error for {fl['number']} (try {attempt + 1}): {e}")
        time.sleep(4)
    best = None
    for f in flights:
        if norm_num(getattr(f, "number", "")) == want_num or norm_num(getattr(f, "callsign", "")) == want_cs:
            o = getattr(f, "origin_airport_iata", "") or ""
            d = getattr(f, "destination_airport_iata", "") or ""
            if o == fl["origin_iata"] or d == fl["dest_iata"]:
                return f
            best = best or f
    return best


# --- AeroDataBox status feed (delays / gate / cancellation) -----------------
def aerodatabox_configured():
    return bool(os.environ.get("AERODATABOX_KEY"))


def aerodatabox_health(fl):
    """Live-probe the AeroDataBox key. Returns (ok, human_readable_reason).

    A key being *present* says nothing about whether it still works — an expired
    or unsubscribed RapidAPI key answers 403 on every call, silently killing
    delay/gate/cancellation alerts. --test probes it so the report can't claim
    a capability the bot does not actually have.
    """
    if not aerodatabox_configured():
        return False, "off (no AeroDataBox key set)"
    key = os.environ.get("AERODATABOX_KEY")
    date = fl["sched_departure_local"].split(" ")[0]
    num = urllib.parse.quote(fl["number"])
    req = urllib.request.Request(
        f"https://{ADB_HOST}/flights/number/{num}/{date}"
        f"?withAircraftImage=false&withLocation=false",
        headers=adb_headers(key),
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            r.read()
        return True, f"on (key verified against {ADB_HOST})"
    except urllib.error.HTTPError as e:
        # Report what the provider said, not a guess mapped from the status
        # code: "not subscribed", "quota exceeded" and "invalid key" all arrive
        # as 403 and have entirely different fixes.
        return False, (f"OFF — {http_error_detail(e)} [host {ADB_HOST}]. "
                       f"Delay/gate/cancellation alerts will NOT fire.")
    except Exception as e:
        return False, (f"unreachable ({type(e).__name__}: {e}) [host {ADB_HOST}]. "
                       f"Delay/gate/cancellation alerts may not fire.")


def _adb_time(obj):
    if not obj:
        return None
    utc = obj.get("utc")
    if not utc:
        return None
    try:
        return datetime.strptime(utc.replace("Z", "").strip(), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def adb_headers(key):
    """Headers for a RapidAPI call.

    The User-Agent matters: the marketplace sits behind Cloudflare, which
    answers urllib's default "Python-urllib/x.y" signature with 403 and
    "error code: 1010" — access denied by bot protection, before the request
    ever reaches the API. That is indistinguishable from an unsubscribed key
    unless the response body is read, and it is why re-subscribing never
    fixed anything. Overridable so a future block can be worked around
    without a code change.
    """
    return {
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": ADB_HOST,
        "User-Agent": os.environ.get("HTTP_USER_AGENT") or DEFAULT_USER_AGENT,
        "Accept": "application/json",
    }


def http_error_detail(e):
    """Status code plus the body the provider sent explaining it.

    RapidAPI answers 403 with a JSON message that distinguishes
    "not subscribed to this API" from "quota exceeded" from a bad key. Logging
    only the status code turns a five-second fix into repeated guesswork.
    """
    code = getattr(e, "code", None)
    body = ""
    try:
        body = re.sub(r"\s+", " ", e.read().decode(errors="replace")).strip()[:280]
    except Exception:
        pass
    return f"HTTP {code}" + (f" — {body}" if body else "")


# --- third-party provider health -------------------------------------------
# A provider that is down should be noticed once, backed off from, and
# announced when it recovers — not retried every cycle forever while the
# capability it powers silently does nothing.
def _health(st, name):
    return st.setdefault("_health", {}).setdefault(name, {"fails": 0})


def provider_in_backoff(st, name, now):
    nxt = _health(st, name).get("next_try_utc")
    if not nxt:
        return False
    try:
        return now < datetime.fromisoformat(nxt)
    except Exception:
        return False


def provider_failed(st, name, label, reason, now):
    h = _health(st, name)
    h["fails"] = h.get("fails", 0) + 1
    h["last_error"] = reason
    h["last_error_utc"] = now.isoformat()
    wait = min(PROVIDER_BACKOFF_MAX_SEC,
               PROVIDER_BACKOFF_START_SEC * (2 ** (h["fails"] - 1)))
    h["next_try_utc"] = (now + timedelta(seconds=wait)).isoformat()
    log(f"   {label} unavailable ({h['fails']}x): {reason} — retrying in {hhmm(wait / 60)}")
    if h["fails"] >= PROVIDER_ALERT_AFTER_FAILS and not h.get("notified"):
        h["notified"] = True
        notify(f"⚠️ <b>{label} unavailable</b>\n{reason}\n\n"
               f"Delay, gate and cancellation alerts are paused until this is fixed.\n"
               f"Departure, landing and en-route updates are unaffected.")


def provider_recovered(st, name, label):
    h = _health(st, name)
    if h.get("notified"):
        h["notified"] = False
        notify(f"✅ <b>{label} restored</b>\nDelay, gate and cancellation alerts are live again.")
        log(f"   {label} recovered")
    h["fails"] = 0
    h["next_try_utc"] = None
    h["last_error"] = None


def provider_down_note(st):
    """One-line degradation notice for the status reply, or ''."""
    h = _health(st, "aerodatabox")
    if h.get("fails", 0) < PROVIDER_ALERT_AFTER_FAILS:
        return ""
    return ("⚠️ <i>Delay, gate and cancellation alerts are unavailable — "
            f"{h.get('last_error', 'status provider down')}</i>")


def fetch_flight_status(fl, st=None, now=None):
    """Parsed status for this leg from AeroDataBox, or None.

    Tracks provider health so a persistent failure backs off, is announced
    once, and is visible in the status reply instead of quietly removing a
    whole category of alerts.
    """
    key = os.environ.get("AERODATABOX_KEY")
    if not key:
        return None
    st = {} if st is None else st
    now = now or datetime.now(timezone.utc)
    if provider_in_backoff(st, "aerodatabox", now):
        return None
    date = lookup_date(fl)
    num = urllib.parse.quote(fl["number"])
    url = (f"https://{ADB_HOST}/flights/number/{num}/{date}"
           f"?withAircraftImage=false&withLocation=false")
    req = urllib.request.Request(url, headers=adb_headers(key))
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        provider_failed(st, "aerodatabox", "AeroDataBox", http_error_detail(e), now)
        return None
    except Exception as e:
        provider_failed(st, "aerodatabox", "AeroDataBox",
                        f"{type(e).__name__}: {e}", now)
        return None
    provider_recovered(st, "aerodatabox", "AeroDataBox")
    items = data if isinstance(data, list) else (data.get("flights") if isinstance(data, dict) else None)
    if not items:
        return None
    chosen = None
    for it in items:
        dep_iata = ((it.get("departure") or {}).get("airport") or {}).get("iata")
        if dep_iata == fl["origin_iata"]:
            chosen = it
            break
    # Whether this is definitely our leg, or a same-numbered flight we fell back
    # to. Only the former is trustworthy enough to overrule the itinerary.
    origin_matched = chosen is not None
    chosen = chosen or items[0]
    dep = chosen.get("departure") or {}
    arr = chosen.get("arrival") or {}
    return {
        "origin_matched": origin_matched,
        "status": (chosen.get("status") or "").strip(),
        "dep_sched": _adb_time(dep.get("scheduledTime")),
        "dep_revised": _adb_time(dep.get("revisedTime") or dep.get("predictedTime") or dep.get("runwayTime")),
        "arr_sched": _adb_time(arr.get("scheduledTime")),
        "arr_revised": _adb_time(arr.get("revisedTime") or arr.get("predictedTime") or arr.get("runwayTime")),
        "dep_gate": dep.get("gate"),
        "dep_terminal": dep.get("terminal"),
        "arr_airport": (arr.get("airport") or {}).get("iata"),
    }


def reconcile_schedule(fl, st, info, home_tz, now):
    """Let the airline's schedule overrule the configured one.

    A departure typed in the traveller's own timezone rather than the departure
    airport's shifts every window, reminder and ETA by the offset between them,
    and nothing downstream can tell: the times look plausible, they are just
    consistently wrong. The provider knows the real schedule, so adopt it and
    say what changed. Returns True when a new correction was announced.
    """
    sched = info.get("dep_sched")
    if not sched or not info.get("origin_matched"):
        return False
    drift = (sched - fl["_dep_utc"]).total_seconds() / 60
    if abs(drift) < SCHEDULE_MISMATCH_MIN:
        return False

    new_dep = sched
    new_arr = info.get("arr_sched") or (fl["_arr_utc"] + timedelta(minutes=drift))
    if new_arr <= new_dep:                       # nonsense pair — keep the duration we know
        new_arr = new_dep + (fl["_arr_utc"] - fl["_dep_utc"])
    old_dep = fl["_dep_utc"]

    fixes = st.setdefault("_schedule", {})
    already = (fixes.get(fl["id"]) or {}).get("dep_utc") == new_dep.isoformat()
    fixes[fl["id"]] = {"dep_utc": new_dep.isoformat(), "arr_utc": new_arr.isoformat(),
                       "source": "aerodatabox", "applied_utc": now.isoformat()}
    fl["_dep_utc"], fl["_arr_utc"] = new_dep, new_arr
    fl["_schedule_corrected"] = True
    if already:
        return False

    # A reminder that already fired, fired at the wrong time. Re-arm it.
    s_pos = st.get(fl["id"])
    if (s_pos and not s_pos.get("departed_alert")
            and now < new_dep - timedelta(minutes=PREDEP_REMINDER_MIN)):
        s_pos["predep_alert"] = False

    # A whole-number-of-hours error is the timezone mistake, not a retimed
    # flight. Naming it is the difference between a fix and a shrug.
    cause = ""
    if abs(drift) >= 45 and min(abs(drift) % 60, 60 - abs(drift) % 60) <= 2:
        cause = (f"\n\nThat is almost exactly {hhmm(abs(drift))} — the itinerary's time looks "
                 f"like it was written in another timezone. <code>sched_departure_local</code> "
                 f"must be local time at {fl['origin_name']}.")
    notify(f"{_flight_header(fl)}\n\n🗓 <b>Schedule corrected</b>\n"
           f"The airline has this departing "
           f"<b>{local_and_home(new_dep, fl['origin_tz'], home_tz)}</b>, not "
           f"{local_and_home(old_dep, fl['origin_tz'], home_tz)} "
           f"({'+' if drift > 0 else '−'}{hhmm(abs(drift))}).{cause}\n\n"
           f"I've switched to the airline's times — reminders, windows and ETAs now "
           f"follow the real flight.")
    log(f"   >> SCHEDULE CORRECTION {fl['number']}: {old_dep.isoformat()} -> "
        f"{new_dep.isoformat()} ({round(drift)}m)")
    return True


def schedule_audit_due(fl, st, now):
    """Whether to spend a call cross-checking this leg's schedule right now."""
    if not aerodatabox_configured():
        return False
    if not (fl["_dep_utc"] - timedelta(hours=SCHEDULE_AUDIT_LEAD_HRS)
            <= now <= fl["_arr_utc"] + timedelta(hours=STATUS_AFTER_ARR_HRS)):
        return False
    last = (st.get("_schedule_audit") or {}).get(fl["id"])
    if not last:
        return True
    try:
        return (now - datetime.fromisoformat(last)).total_seconds() >= SCHEDULE_AUDIT_SEC
    except Exception:
        return True


def audit_schedule(fl, st, home_tz, now):
    """Cheap standalone schedule check, for the days before the status window
    opens. Without it a wrong departure time is only noticed once the flight is
    nearly due — or, if the error is large enough, never, because the windows
    derived from it never open at the right moment."""
    info = fetch_flight_status(fl, st, now)
    st.setdefault("_schedule_audit", {})[fl["id"]] = now.isoformat()
    if info and not reconcile_schedule(fl, st, info, home_tz, now):
        log(f"   {fl['number']}: schedule audit — departs "
            f"{local_and_home(fl['_dep_utc'], fl['origin_tz'], home_tz)}")
    save_state(st)


def process_status(fl, st, home_tz, now):
    """Poll AeroDataBox (rate-limited) and alert on delay/gate/cancel/divert changes."""
    if not aerodatabox_configured():
        return
    key = fl["id"] + "#status"
    s = st.setdefault(key, {
        "last_check": None, "gate": None, "terminal": None,
        "dep_delay_alerted": None, "arr_delay_alerted": None,
        "cancel_alerted": False, "divert_alerted": False, "baseline": False,
    })
    last = s.get("last_check")
    if last is not None:
        try:
            if (now - datetime.fromisoformat(last)).total_seconds() < STATUS_POLL_SEC:
                return
        except Exception:
            pass

    info = fetch_flight_status(fl, st, now)
    s["last_check"] = now.isoformat()
    if not info:
        save_state(st)
        return
    # Before reading anything else off this response: is it even the same flight
    # the itinerary describes? Everything below is timed off _dep_utc.
    st.setdefault("_schedule_audit", {})[fl["id"]] = now.isoformat()
    reconcile_schedule(fl, st, info, home_tz, now)

    header = _flight_header(fl)
    status_l = info["status"].lower()

    # Authoritative landing from the airline status feed (reliable, unlike a radar drop).
    if "arrived" in status_l:
        s_pos = st.get(fl["id"])
        if s_pos and not s_pos.get("landed_alert"):
            _fire_landing(fl, s_pos, header, home_tz, now, "AeroDataBox: Arrived")
            save_state(st)

    # Cancellation
    if ("cancel" in status_l) and not s.get("cancel_alerted"):
        s["cancel_alerted"] = True
        notify(f"{header}\n\n🛑 <b>CANCELLED</b>\n{fl['origin_name']} → {fl['dest_name']}\n"
               f"Airline status: {info['status']}. Check with China Southern for rebooking.")
        log(f"   >> CANCELLATION alert sent for {fl['number']}")

    # Diversion
    diverted = ("divert" in status_l) or (info["arr_airport"] and info["arr_airport"] != fl["dest_iata"])
    if diverted and not s.get("divert_alerted"):
        s["divert_alerted"] = True
        to = info["arr_airport"] or "another airport"
        notify(f"{header}\n\n🛑 <b>DIVERTED</b>\nNow routing to {to} instead of {fl['dest_iata']}.")
        log(f"   >> DIVERSION alert sent for {fl['number']}")

    # Departure delay (revised vs scheduled)
    if info["dep_revised"] and info["dep_sched"]:
        delay = (info["dep_revised"] - info["dep_sched"]).total_seconds() / 60
        prev = s.get("dep_delay_alerted")
        if delay >= DELAY_ALERT_MIN and (prev is None or abs(delay - prev) >= DELAY_REALERT_MIN):
            s["dep_delay_alerted"] = delay
            notify(f"{header}\n\n🕒 <b>DEPARTURE DELAYED</b> +{hhmm(delay)}\n"
                   f"New departure: {local_and_home(info['dep_revised'], fl['origin_tz'], home_tz)}\n"
                   f"(was {local_and_home(info['dep_sched'], fl['origin_tz'], home_tz)})")
            log(f"   >> DEP DELAY alert sent for {fl['number']} (+{int(delay)}m)")

    # Arrival delay (only meaningful, avoids double-noise with dep delay)
    if info["arr_revised"] and info["arr_sched"]:
        adelay = (info["arr_revised"] - info["arr_sched"]).total_seconds() / 60
        prev = s.get("arr_delay_alerted")
        if adelay >= ARR_DELAY_ALERT_MIN and (prev is None or abs(adelay - prev) >= DELAY_REALERT_MIN):
            s["arr_delay_alerted"] = adelay
            notify(f"{header}\n\n🕒 <b>ARRIVING LATER</b> +{hhmm(adelay)}\n"
                   f"New ETA {fl['dest_name']}: {local_and_home(info['arr_revised'], fl['dest_tz'], home_tz)}\n"
                   f"(was {local_and_home(info['arr_sched'], fl['dest_tz'], home_tz)})")
            log(f"   >> ARR DELAY alert sent for {fl['number']} (+{int(adelay)}m)")

    # Gate / terminal changes (baseline the first time, alert on later changes)
    if info["dep_gate"]:
        if s.get("baseline") and s.get("gate") and info["dep_gate"] != s["gate"]:
            notify(f"{header}\n\n🚪 <b>GATE CHANGE</b> at {fl['origin_name']}\n"
                   f"{s['gate']} → <b>{info['dep_gate']}</b>")
            log(f"   >> GATE change alert sent for {fl['number']}")
        s["gate"] = info["dep_gate"]
    if info["dep_terminal"]:
        if s.get("baseline") and s.get("terminal") and info["dep_terminal"] != s["terminal"]:
            notify(f"{header}\n\n🚪 <b>TERMINAL CHANGE</b> at {fl['origin_name']}\n"
                   f"T{s['terminal']} → <b>T{info['dep_terminal']}</b>")
            log(f"   >> TERMINAL change alert sent for {fl['number']}")
        s["terminal"] = info["dep_terminal"]

    # Share latest ETA + status text so the progress updates can use them.
    s["baseline"] = True
    s["status_text"] = info["status"]
    eta = info["arr_revised"] or info["arr_sched"]
    s["eta_utc"] = eta.isoformat() if eta else None
    if info["arr_revised"] and info["arr_sched"]:
        s["arr_delta_min"] = round((info["arr_revised"] - info["arr_sched"]).total_seconds() / 60)
    log(f"   {fl['number']}: status='{info['status']}' gate={info['dep_gate']} term={info['dep_terminal']}")
    save_state(st)


# --- position state machine (departure / landing) ---------------------------
def in_window(fl, now):
    start = fl["_dep_utc"] - timedelta(minutes=WINDOW_BEFORE_DEP_MIN)
    end = fl["_arr_utc"] + timedelta(hours=WINDOW_AFTER_ARR_HRS)
    return start <= now <= end


def status_window_open(fl, now):
    start = fl["_dep_utc"] - timedelta(hours=STATUS_BEFORE_DEP_HRS)
    end = fl["_arr_utc"] + timedelta(hours=STATUS_AFTER_ARR_HRS)
    return start <= now <= end


def process_flight(api, fl, st, home_tz, now):
    fid = fl["id"]
    s = st.setdefault(fid, {
        "phase": "WAITING", "predep_alert": False, "departed_alert": False,
        "landed_alert": False, "seen_airborne": False, "missing_polls": 0,
        "last_alt": None, "last_seen_utc": None,
    })
    s.setdefault("predep_alert", False)
    if s["phase"] == "LANDED":
        # Self-heal a FALSE landing (e.g. a mid-flight radar-coverage gap wrongly
        # read as a landing). If it wasn't a confirmed landing and we're still well
        # before arrival, the flight can't have landed — resume tracking.
        eta, _ = _eta_and_note(fl, st)
        if not s.get("landed_confirmed") and now < eta - timedelta(minutes=30):
            log(f"   {fl['number']}: reverting FALSE landing "
                f"({int((eta - now).total_seconds()/60)}m before ETA) — resuming tracking")
            s["phase"] = "AIRBORNE"
            s["landed_alert"] = False
            s["missing_polls"] = 0
        else:
            return

    header = _flight_header(fl)

    mins_to_dep = (fl["_dep_utc"] - now).total_seconds() / 60
    if (not s["predep_alert"] and not s["departed_alert"]
            and -10 <= mins_to_dep <= PREDEP_REMINDER_MIN):
        s["predep_alert"] = True
        notify(f"{header}\n\n⏰ <b>Departs in ~{max(0, round(mins_to_dep))} min</b>\n"
               f"{fl['origin_name']} → {fl['dest_name']}\n"
               f"Scheduled departure: {local_and_home(fl['_dep_utc'], fl['origin_tz'], home_tz)}\n"
               f"I'll ping you the moment it's airborne. 🛫")
        log(f"   >> T-30 reminder sent for {fl['number']}")

    f = find_live_flight(api, fl)
    if f is not None:
        alt = getattr(f, "altitude", 0) or 0
        spd = getattr(f, "ground_speed", 0) or 0
        on_ground = bool(getattr(f, "on_ground", 0))
        s["last_alt"] = alt
        s["last_seen_utc"] = now.isoformat()
        s["missing_polls"] = 0
        airborne = (not on_ground) and alt >= AIRBORNE_MIN_ALT_FT and spd > 50
        log(f"   {fl['number']}: live onground={on_ground} alt={alt} spd={spd} "
            f"{getattr(f,'origin_airport_iata','?')}->{getattr(f,'destination_airport_iata','?')}")
        if airborne:
            s["seen_airborne"] = True
            if s["phase"] == "WAITING" and not s["departed_alert"]:
                s["phase"] = "AIRBORNE"
                s["departed_alert"] = True
                s["departed_utc"] = now.isoformat()
                notify(f"{header}\n\n🛫 <b>DEPARTED</b> {fl['origin_name']}\n"
                       f"Now airborne toward {fl['dest_name']}.\n"
                       f"Altitude {alt:,} ft · {spd} kt\n"
                       f"Time: {local_and_home(now, fl['origin_tz'], home_tz)}\n"
                       f"Scheduled arrival: {local_and_home(fl['_arr_utc'], fl['dest_tz'], home_tz)}")
                log(f"   >> DEPARTURE alert sent for {fl['number']}")
            # Proactive "how's the trip going" updates while en route.
            # Refresh the live ETA before the progress update, so any milestone
            # message quotes the current arrival time rather than a stale one.
            try:
                refresh_live_eta(api, fl, st, header, home_tz, now, f)
            except Exception:
                log("!! live ETA error:\n" + traceback.format_exc())
            process_progress(fl, s, st, header, home_tz, now, alt, spd)
        elif (on_ground and s["seen_airborne"] and _near_arrival(fl, st, now)
              and status_confidence(fl, st) != "enroute"):
            # Actually observed on the ground near the destination — an
            # observation, not an inference, so this one is confirmed.
            _fire_landing(fl, s, header, home_tz, now, "on-ground near destination")
    else:
        if s["seen_airborne"] and s["phase"] == "AIRBORNE":
            s["missing_polls"] += 1
            # A radar dropout is a plausible landing ONLY if ALL hold:
            #   1) we're within ~45 min of the (live) ETA,
            #   2) at least 85% of the flight has elapsed, and
            #   3) AeroDataBox does not still report the flight en route.
            # Otherwise it's a coverage gap over remote airspace — NOT a landing.
            near = _near_arrival(fl, st, now)
            frac = _elapsed_frac(fl, s, st, now)
            conf = status_confidence(fl, st)
            if conf == "enroute":
                # The airline says it is still flying. Radar silence means a
                # coverage gap, nothing more.
                allow, need = False, MISSING_POLLS_FOR_LANDING
            elif conf == "arrived":
                allow, need = True, MISSING_POLLS_FOR_LANDING
            else:
                # No corroboration available. Require nearly the whole flight to
                # have elapsed and a much longer silence before inferring
                # anything, and mark the result unconfirmed so it can self-heal.
                allow = near and frac >= MIN_ELAPSED_FRAC_NO_STATUS
                need = MISSING_POLLS_NO_STATUS
            log(f"   {fl['number']}: not in feed ({s['missing_polls']}/{need}) "
                f"near_arrival={near} elapsed={frac:.0%} status={conf} allow_land={allow}")
            if allow and s["missing_polls"] >= need:
                _fire_landing(fl, s, header, home_tz, now,
                              "dropped off radar near destination",
                              confirmed=(conf == "arrived"))
        else:
            log(f"   {fl['number']}: not airborne yet / not in feed")


def fr24_live_eta(api, f):
    """FlightRadar24's own running arrival estimate (UTC), or None.

    The live feed row carries only position; the details endpoint is what
    exposes the estimate that moves as the aircraft speeds up or reroutes.
    Failures return None, but they say why first — swallowing them silently
    made an earlier outage of this path impossible to diagnose after the fact.
    """
    try:
        d = api.get_flight_details(f)
    except Exception as e:
        log(f"   FR24 details error for {getattr(f, 'id', '?')}: {type(e).__name__}: {e}")
        return None
    if not isinstance(d, dict):
        log(f"   FR24 details: unexpected payload type {type(d).__name__}")
        return None
    times = d.get("time")
    if not isinstance(times, dict):
        log(f"   FR24 details: no 'time' block (keys: {sorted(d)[:8]})")
        return None
    for group, field in (("estimated", "arrival"), ("other", "eta"), ("real", "arrival")):
        node = times.get(group)
        ts = node.get(field) if isinstance(node, dict) else None
        if ts:
            try:
                return datetime.fromtimestamp(int(ts), tz=timezone.utc)
            except Exception:
                continue
    log(f"   FR24 details: no arrival estimate yet (time groups: {sorted(times)})")
    return None


def _haversine_nm(lat1, lon1, lat2, lon2):
    r = 3440.065  # earth radius in nautical miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _dest_coords(api, fl, st):
    """Destination lat/lon, looked up once and cached in state."""
    cache = st.setdefault(fl["id"] + "#fr24", {})
    if cache.get("dest_lat") is not None:
        return cache["dest_lat"], cache["dest_lon"]
    try:
        ap = api.get_airport(fl["dest_iata"])
        lat = float(getattr(ap, "latitude", None))
        lon = float(getattr(ap, "longitude", None))
    except Exception as e:
        log(f"   airport lookup failed for {fl['dest_iata']}: {type(e).__name__}: {e}")
        return None, None
    cache["dest_lat"], cache["dest_lon"] = lat, lon
    return lat, lon


def position_eta(api, fl, st, now, f):
    """ETA from distance-to-run over ground speed.

    Independent of the details endpoint, and recomputed from the position we
    already fetch each poll — so the estimate keeps moving even when FR24
    publishes no arrival time of its own.
    """
    gs = getattr(f, "ground_speed", 0) or 0
    lat, lon = getattr(f, "latitude", None), getattr(f, "longitude", None)
    if gs < 100 or lat is None or lon is None:
        return None                      # taxiing, or no usable fix
    dlat, dlon = _dest_coords(api, fl, st)
    if dlat is None:
        return None
    nm = _haversine_nm(lat, lon, dlat, dlon)
    # Add a little for descent, approach and taxi-in, which cruise speed overstates.
    return now + timedelta(hours=nm / gs) + timedelta(minutes=12)


def refresh_live_eta(api, fl, st, header, home_tz, now, f):
    """Keep the arrival estimate current mid-flight, and speak up when it moves."""
    ss = st.setdefault(fl["id"] + "#fr24", {})
    last = ss.get("polled_utc")
    if last:
        try:
            if (now - datetime.fromisoformat(last)).total_seconds() < FR24_ETA_POLL_SEC:
                return
        except Exception:
            pass
    ss["polled_utc"] = now.isoformat()
    eta = fr24_live_eta(api, f)
    source = "FlightRadar24 estimate"
    if eta is None:
        # FR24 does not publish an arrival time for every flight, so fall back
        # to one we can compute ourselves from the position we already have.
        eta = position_eta(api, fl, st, now, f)
        source = "distance/ground-speed estimate"
    if eta is None:
        return
    ss["eta_utc"] = eta.isoformat()
    ss["eta_source"] = source

    # Compare against what the user was last told to expect — the previous
    # announced ETA, or the printed schedule if nothing has been announced yet.
    told = ss.get("told_eta_utc")
    try:
        baseline = datetime.fromisoformat(told) if told else fl["_arr_utc"]
    except Exception:
        baseline = fl["_arr_utc"]
    shift = (eta - baseline).total_seconds() / 60
    # A couple of minutes of jitter on every refresh is noise, not news.
    if abs(shift) < ETA_SHIFT_ALERT_MIN:
        ss.setdefault("told_eta_utc", eta.isoformat())
        return
    ss["told_eta_utc"] = eta.isoformat()
    notify(f"{header}\n\n🕒 <b>Arrival time updated</b>\n"
           f"Now landing {local_and_home(eta, fl['dest_tz'], home_tz)}\n"
           f"~{hhmm(abs(shift))} {'later' if shift > 0 else 'earlier'} than the last estimate\n"
           f"<i>{ss.get('eta_source', 'live estimate')}</i>")
    log(f"   >> ETA shift alert for {fl['number']} ({shift:+.0f} min)")


def _eta_and_note(fl, st):
    """Best-known arrival ETA (UTC) and an on-schedule note, using AeroDataBox if present."""
    ss = st.get(fl["id"] + "#status", {})
    eta = None
    if ss.get("eta_utc"):
        try:
            eta = datetime.fromisoformat(ss["eta_utc"])
        except Exception:
            eta = None
    if eta is not None:
        delta = ss.get("arr_delta_min")
        if delta is None:
            note = "on schedule"
        elif delta <= -10:
            note = f"~{abs(delta)} min ahead of schedule"
        elif delta >= 15:
            note = f"running ~{hhmm(delta)} late"
        else:
            note = "on schedule"
        return eta, note

    # FlightRadar24's live estimate — the one that keeps moving during the
    # flight, so a plane that makes up or loses time does not arrive as a
    # surprise. Preferred over anything derived from the printed schedule.
    fs = st.get(fl["id"] + "#fr24", {})
    if fs.get("eta_utc"):
        try:
            eta = datetime.fromisoformat(fs["eta_utc"])
            delta = round((eta - fl["_arr_utc"]).total_seconds() / 60)
            if delta >= 15:
                note = f"live estimate · ~{hhmm(delta)} later than scheduled"
            elif delta <= -10:
                note = f"live estimate · ~{hhmm(abs(delta))} earlier than scheduled"
            else:
                note = "live estimate · on schedule"
            return eta, note
        except Exception:
            pass

    # No live status feed (no AeroDataBox key, or it is failing). Returning the
    # printed schedule labelled "as scheduled" asserts on-time with no evidence
    # — and after a late push-back it is simply wrong, which also drags the
    # landing heuristics early because they key off this ETA. If departure was
    # actually observed, project from it and say plainly that it is an estimate.
    s = st.get(fl["id"], {})
    if s.get("departed_utc"):
        try:
            dep = datetime.fromisoformat(s["departed_utc"])
            eta = dep + (fl["_arr_utc"] - fl["_dep_utc"])
            late = round((dep - fl["_dep_utc"]).total_seconds() / 60)
            if late >= 15:
                return eta, f"estimated · departed ~{hhmm(late)} late (no live status feed)"
            return eta, "estimated from actual departure (no live status feed)"
        except Exception:
            pass
    return fl["_arr_utc"], "scheduled time — no live status feed"


def process_progress(fl, s, st, header, home_tz, now, alt, spd):
    """While airborne, send occasional en-route + on-approach updates (fires each once)."""
    try:
        departed = datetime.fromisoformat(s["departed_utc"]) if s.get("departed_utc") else fl["_dep_utc"]
    except Exception:
        departed = fl["_dep_utc"]
    eta, note = _eta_and_note(fl, st)
    eta_txt = local_and_home(eta, fl["dest_tz"], home_tz)
    mins_to_eta = (eta - now).total_seconds() / 60

    # On-approach / descending (once), when close to ETA or clearly descending near the end.
    if not s.get("approach_sent"):
        descending = alt and alt < 13000 and mins_to_eta <= 60
        if mins_to_eta <= APPROACH_BEFORE_ARR_MIN or descending:
            s["approach_sent"] = True
            notify(f"{header}\n\n🛬 <b>On approach to {fl['dest_name']}</b>\n"
                   f"Descending now · landing ~{eta_txt}\nStatus: {note}. Touchdown confirmation to follow.")
            log(f"   >> APPROACH update sent for {fl['number']}")
            return

    # En-route milestones (only for longer flights, to avoid noise on short hops).
    total_min = (eta - departed).total_seconds() / 60
    if total_min < PROGRESS_MIN_DURATION_MIN:
        return
    frac = (now - departed).total_seconds() / 60 / total_min if total_min > 0 else 0
    done = set(s.get("progress_sent", []))
    for m in PROGRESS_FRACTIONS:
        tag = str(int(m * 100))
        if frac >= m and tag not in done and mins_to_eta > APPROACH_BEFORE_ARR_MIN:
            done.add(tag)
            s["progress_sent"] = sorted(done)
            alt_txt = f"{alt:,} ft · {spd} kt" if alt else "en route"
            notify(f"{header}\n\n🧭 <b>En route — ~{tag}% there</b>\n"
                   f"Toward {fl['dest_name']} · {alt_txt}\n"
                   f"Status: {note} · ETA {eta_txt}")
            log(f"   >> PROGRESS {tag}% update sent for {fl['number']}")
            return  # at most one progress message per poll


def _near_arrival(fl, st, now):
    """True once we're within LANDING_NEAR_ETA_MIN of the (best-known) arrival time."""
    eta, _ = _eta_and_note(fl, st)
    return now >= eta - timedelta(minutes=LANDING_NEAR_ETA_MIN)


# Airline statuses that mean the flight is definitely NOT on the ground at destination.
# If AeroDataBox reports any of these, a FlightRadar radar-dropout must NOT be read as a
# landing — the status feed is the authority; we wait for "Arrived" or a genuine touchdown.
_ENROUTE_STATUSES = ("expected", "enroute", "en route", "departed", "boarding",
                     "delayed", "scheduled", "checkin", "check-in", "gate", "active", "airborne")


def status_confidence(fl, st):
    """What the airline feed actually asserts: 'arrived', 'enroute' or 'unknown'.

    'unknown' is deliberately not the same as 'enroute is false'. Treating a
    silent feed as consent is what allowed a radar dropout to be announced as a
    landing while the aircraft was still flying: the provider was returning 403
    on every call, so nothing contradicted the guess and it was read as
    permission. Absence of evidence now demands more evidence, not less.
    """
    txt = (st.get(fl["id"] + "#status", {}).get("status_text") or "").lower()
    if not txt:
        return "unknown"
    if "arrived" in txt or "landed" in txt:
        return "arrived"
    if any(k in txt for k in _ENROUTE_STATUSES):
        return "enroute"
    return "unknown"


def _elapsed_frac(fl, s, st, now):
    try:
        departed = datetime.fromisoformat(s["departed_utc"]) if s.get("departed_utc") else fl["_dep_utc"]
    except Exception:
        departed = fl["_dep_utc"]
    eta, _ = _eta_and_note(fl, st)
    total = (eta - departed).total_seconds()
    return ((now - departed).total_seconds() / total) if total > 0 else 1.0


def _fire_landing(fl, s, header, home_tz, now, reason, confirmed=True):
    """Announce a landing.

    `confirmed` must be False when the landing was inferred rather than
    observed. It used to be set True unconditionally, which silently disabled
    the self-heal that exists precisely to walk back a wrong guess — and made a
    radar-dropout inference indistinguishable from an airline confirmation.
    """
    if s["landed_alert"]:
        return
    s["phase"] = "LANDED"
    s["landed_alert"] = True
    s["landed_confirmed"] = bool(confirmed)
    s["landed_utc"] = now.isoformat()
    delta = round((now - fl["_arr_utc"]).total_seconds() / 60)
    punct = f"{abs(delta)} min early" if delta <= -5 else (f"{delta} min late" if delta >= 15 else "on time")
    caveat = ("" if confirmed else
              "\n<i>Inferred from loss of radar contact — not yet confirmed by "
              "the airline feed. I'll correct this if it turns out to still be flying.</i>")
    notify(f"{header}\n\n🛬 <b>LANDED</b> at {fl['dest_name']}\n"
           f"Time: {local_and_home(now, fl['dest_tz'], home_tz)}\n"
           f"Scheduled: {local_and_home(fl['_arr_utc'], fl['dest_tz'], home_tz)} ({punct})"
           f"{caveat}")
    log(f"   >> LANDING alert sent for {fl['number']} ({reason}, "
        f"{'confirmed' if confirmed else 'UNCONFIRMED'})")


# --- inbound Telegram commands (ask the bot for a live update) --------------
def telegram_get_updates(offset=None):
    """Fetch new messages sent TO the bot. Returns [] on any problem."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        return []
    q = {"timeout": 0}
    if offset is not None:
        q["offset"] = offset
    try:
        with urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/getUpdates?" + urllib.parse.urlencode(q),
                timeout=20) as r:
            d = json.loads(r.read().decode())
        return d.get("result", []) if d.get("ok") else []
    except Exception as e:
        log(f"   telegram getUpdates error: {e}")
        return []


_LEG_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)\s+(.*)$")


def _leg_parts(fl):
    """Split a "N/M  Origin → Dest" label into (route, spelled-out leg marker).

    Printed bare beside the flight number, "2/2" reads as a completion counter
    ("2 of 2 done") rather than "leg 2 of 2" — so spell it out and move it off
    the headline.
    """
    m = _LEG_RE.match(fl.get("leg") or "")
    if not m:
        return ((fl.get("leg") or fl.get("trip") or "").strip(), "")
    return m.group(3).strip(), f"leg {m.group(1)} of {m.group(2)}"


def _flight_header(fl):
    """Alert headline. Same spelled-out leg treatment as the status reply, so a
    departure alert can't be misread as "2 of 2 flights done" either."""
    route, leg = _leg_parts(fl)
    return f"✈️ <b>{fl['number']}</b> {route}" + (f"\n<i>{leg}</i>" if leg else "")


def human_duration(mins):
    """Like hhmm() but rolls into days — "17d 5h" beats "413h 51m"."""
    mins = int(round(abs(mins)))
    days, rem = divmod(mins, 1440)
    hrs = rem // 60
    if days:
        return f"{days}d {hrs}h" if hrs else f"{days}d"
    return hhmm(mins)


def mentioned_flights(data, text):
    """Legs the message actually refers to, by flight number or place name."""
    t = (text or "").lower()
    hits = []
    for fl in data["flights"]:
        num = fl["number"].lower()
        keys = {num, num.replace(" ", ""), "".join(c for c in num if c.isdigit())}
        route, _ = _leg_parts(fl)
        for src in (route, fl.get("origin_name", ""), fl.get("dest_name", "")):
            for word in re.split(r"[^\w]+", src.lower()):
                if len(word) > 3:          # skip "t1", "→", airport codes
                    keys.add(word)
        if any(k and k in t for k in keys):
            hits.append(fl)
    return hits


def _finished_recently(fl, st, now):
    """A leg that landed within the last RECENT_LEG_HRS. Filtering completed
    legs out of the default view made a flight that finished hours ago look like
    it was never in the itinerary at all — the one reading that is never true."""
    s = st.get(fl["id"], {})
    if s.get("phase") != "LANDED":
        return False
    when = s.get("landed_utc") or s.get("last_seen_utc")
    if not when:
        # Landed before landing times were recorded; fall back to the schedule.
        when = fl["_arr_utc"].isoformat()
    try:
        return (now - datetime.fromisoformat(when)) <= timedelta(hours=RECENT_LEG_HRS)
    except Exception:
        return False


def focus_flights(data, st, now=None):
    """What to show when nothing specific was asked for: whatever is in the air,
    what just finished, and the next departure. Printing four legs — three of
    them weeks out — on every question buries the one answer being looked for."""
    now = now or datetime.now(timezone.utc)
    airborne = [fl for fl in data["flights"]
                if st.get(fl["id"], {}).get("phase") == "AIRBORNE"]
    recent = [fl for fl in data["flights"] if _finished_recently(fl, st, now)]
    waiting = sorted((fl for fl in data["flights"]
                      if st.get(fl["id"], {}).get("phase", "WAITING") == "WAITING"),
                     key=lambda f: f["_dep_utc"])
    picked = recent + airborne + (waiting[:1] if not airborne else [])
    # Preserve itinerary order, and never answer with nothing.
    return [fl for fl in data["flights"] if fl in picked] or data["flights"]


def build_status_text(api, data, st, now, live=False, only=None):
    """Human-readable flight state; `live` also hits FR24 for position.

    `only` limits the body to a subset; the completed count still reflects the
    whole trip so the headline never lies about overall progress.
    """
    home_tz = data["home_tz"]
    all_flights = data["flights"]
    flights = all_flights if only is None else only
    done = sum(1 for fl in all_flights if st.get(fl["id"], {}).get("phase") == "LANDED")
    lines = [f"✈️ <b>Flight status</b>",
             f"<b>{done} of {len(all_flights)}</b> legs completed"]
    for fl in flights:
        s = st.get(fl["id"], {})
        phase = s.get("phase", "WAITING")
        route, leg = _leg_parts(fl)
        icon = {"WAITING": "🕐", "AIRBORNE": "🛫", "LANDED": "🛬"}.get(phase, "•")
        lines.append(f"\n{icon} <b>{fl['number']}</b> {route}")
        if leg:
            lines.append(f"<i>{leg}</i>")
        if phase == "LANDED":
            when = s.get("landed_utc")
            if when:
                try:
                    lines.append("✅ Landed "
                                 f"{local_and_home(datetime.fromisoformat(when), fl['dest_tz'], home_tz)}")
                except Exception:
                    lines.append("✅ Landed")
            else:
                lines.append("✅ Landed")
            continue
        eta, note = _eta_and_note(fl, st)
        if phase == "AIRBORNE":
            lines.append(f"🛫 In the air — ETA {local_and_home(eta, fl['dest_tz'], home_tz)}")
            left = (eta - now).total_seconds() / 60
            if left > 0:
                lines.append(f"~{human_duration(left)} remaining · {note}")
            if live:
                f = find_live_flight(api, fl)
                if f is not None:
                    alt = getattr(f, "altitude", 0) or 0
                    spd = getattr(f, "ground_speed", 0) or 0
                    lines.append(f"📍 {alt:,} ft · {spd} kt")
                else:
                    lines.append("📍 not in the live radar feed right now")
        else:
            togo = (fl["_dep_utc"] - now).total_seconds() / 60
            if togo > 0:
                lines.append(f"⏳ Not departed yet — departs "
                             f"{local_and_home(fl['_dep_utc'], fl['origin_tz'], home_tz)}")
                lines.append(f"in ~{human_duration(togo)}")
            else:
                # Scheduled departure has passed with nothing observed. Saying
                # "not departed yet" here would be a straight falsehood.
                lines.append(f"❓ No sighting — was due "
                             f"{local_and_home(fl['_dep_utc'], fl['origin_tz'], home_tz)}, "
                             f"{human_duration(-togo)} ago")
                lines.append("<i>Nothing on radar or the airline feed for this leg.</i>")
        if fl.get("_schedule_corrected"):
            lines.append("<i>times from the airline feed — the itinerary had a "
                         "different departure</i>")
    degraded = provider_down_note(st)
    if degraded:
        lines.append("\n" + degraded)
    hidden = len(all_flights) - len(flights)
    if hidden > 0:
        lines.append(f"\n<i>+{hidden} other leg{'s' if hidden > 1 else ''} — "
                     f"say “all” for the full trip, or name a flight</i>")
    return "\n".join(lines)


def select_flights(data, st, text):
    """Which legs a message wants: the ones it names, everything if it asks for
    everything, otherwise just what is actually happening."""
    named = mentioned_flights(data, text)
    if named:
        return named
    if re.search(r"\b(all|everything|full|itinerary|trip|every)\b", (text or "").lower()):
        return None                     # None == no filtering
    return focus_flights(data, st)


HELP_TEXT = ("🤖 <b>Flight monitor</b>\n\n"
             "/status — where every flight stands\n"
             "/where — same, plus live altitude &amp; speed\n"
             "/help — this message\n\n"
             "You can also just ask, e.g. <i>“how long till the next flight?”</i>, "
             "<i>“is it on schedule?”</i>, <i>“when does it land?”</i>, <i>“where is it?”</i>\n\n"
             "By default I answer about the leg in progress (or the next one). "
             "Name a flight or city to ask about it — <i>“CZ626”</i>, "
             "<i>“the Bali flight”</i> — or say <i>“all”</i> for the whole trip.\n\n"
             "I also ping you automatically on departure, landing, "
             "en-route milestones and (when available) delays and gate changes.")

# Plain-language routing. Every question people actually ask ("how long?",
# "on time?", "when does it land?") is already answered by the status reply —
# it carries time-to-departure, ETA and the on-schedule note — so the only gap
# is recognising the question. Keyword matching does that deterministically,
# with no model API to add latency, cost, or a mid-trip failure mode.
_LIVE_WORDS = ("where", "position", "altitude", "how high", "speed", "how fast",
               "live", "map", "over ", "flying")
_STATUS_WORDS = ("how long", "when", "next", "schedule", "on time", "ontime",
                 "delay", "late", "eta", "arriv", "land", "status", "flight",
                 "left", "remaining", "time", "long till", "long until")


def match_intent(text):
    """Map a slash command or a plain-language question to an intent."""
    t = text.lower().strip()
    words = t.lstrip("/").split()
    first = words[0].split("@")[0] if words else ""
    if first in ("help", "start", "commands"):
        return "help"
    if first in ("where", "live", "position"):
        return "live"
    if first in ("status", "flights", "eta"):
        return "status"
    # Longest-standing phrasings first: a position question mentioning "land"
    # should still be answered as a position question.
    if any(k in t for k in _LIVE_WORDS):
        return "live"
    if any(k in t for k in _STATUS_WORDS):
        return "status"
    return None


def handle_incoming(api, data, st, now):
    """Answer commands texted to the bot. Telegram only; owner's chat only."""
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not (os.environ.get("TELEGRAM_BOT_TOKEN") and chat_id):
        return
    for u in telegram_get_updates(st.get("_tg_offset")):
        # Advance the offset even for messages we ignore, so one unparseable
        # or foreign message can't wedge the queue forever.
        st["_tg_offset"] = u["update_id"] + 1
        msg = u.get("message") or u.get("edited_message") or {}
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        # Anyone can find a bot by its @username, so only the configured chat
        # gets answers — never leak itinerary details to a stranger.
        if str((msg.get("chat") or {}).get("id", "")) != chat_id:
            log("   ignoring inbound message from a non-owner chat")
            continue
        intent = match_intent(text)
        log(f"   inbound message → intent={intent or 'unrecognised'}")
        if intent in ("status", "live"):
            only = select_flights(data, st, text)
            send_telegram(build_status_text(api, data, st, now,
                                            live=(intent == "live"), only=only))
        elif intent == "help":
            send_telegram(HELP_TEXT)
        else:
            # Say what wasn't understood rather than silently dumping help.
            send_telegram("🤔 I didn't catch that.\n\n" + HELP_TEXT)


def poll_cycle(api, data, st, now):
    home_tz = data["home_tz"]
    any_active = False
    try:
        handle_incoming(api, data, st, now)
    except Exception:
        log("!! inbound command error:\n" + traceback.format_exc())
    for fl in data["flights"]:
        # Skip only CONFIRMED landings; a bare "LANDED" phase may be a false
        # radar-dropout landing that process_flight will self-heal.
        if st.get(fl["id"], {}).get("landed_confirmed"):
            continue
        if status_window_open(fl, now):
            any_active = True
            try:
                process_status(fl, st, home_tz, now)
            except Exception:
                log("!! status error:\n" + traceback.format_exc())
        elif schedule_audit_due(fl, st, now):
            try:
                audit_schedule(fl, st, home_tz, now)
            except Exception:
                log("!! schedule audit error:\n" + traceback.format_exc())
        if in_window(fl, now):
            any_active = True
            process_flight(api, fl, st, home_tz, now)
    save_state(st)
    return any_active


def cmd_status(data, st):
    print(f"\nBooking {data.get('booking','?')} — {data.get('passenger','?')}\n")
    now = datetime.now(timezone.utc)
    for fl in data["flights"]:
        s = st.get(fl["id"], {})
        win = "OPEN" if in_window(fl, now) else ("past" if now > fl["_arr_utc"] else "upcoming")
        print(f"  {fl['number']}  {fl['leg']:<28}  dep {local_and_home(fl['_dep_utc'], fl['origin_tz'], data['home_tz'])}")
        print(f"        phase={s.get('phase','WAITING'):<9} window={win:<9} "
              f"predep={s.get('predep_alert',False)} dep={s.get('departed_alert',False)} land={s.get('landed_alert',False)}")
    print()


def announce_itinerary(data, st):
    """Confirm the itinerary whenever it changes.

    The flight list lives in an encrypted secret and is read once at startup, so
    an edit is invisible until something either alerts or doesn't — there was no
    way to tell a saved change from a lost one except by waiting for a flight to
    be missed. Say what was loaded, once, when it differs from last time.
    """
    sig = [f"{fl['id']}@{fl['_dep_utc'].isoformat()}" for fl in data["flights"]]
    if st.get("_itinerary_sig") == sig:
        return
    first = "_itinerary_sig" not in st
    st["_itinerary_sig"] = sig
    save_state(st)
    log(f"   itinerary: {len(sig)} legs — " + ", ".join(fl["number"] for fl in data["flights"]))
    title = "Itinerary loaded" if first else "Itinerary updated"
    lines = [f"📋 <b>{title}</b> — {len(data['flights'])} legs"]
    for fl in data["flights"]:
        phase = st.get(fl["id"], {}).get("phase", "WAITING")
        icon = {"WAITING": "🕐", "AIRBORNE": "🛫", "LANDED": "✅"}.get(phase, "•")
        route, _ = _leg_parts(fl)
        lines.append(f"{icon} <b>{fl['number']}</b> {route} — "
                     f"{local_and_home(fl['_dep_utc'], fl['origin_tz'], data['home_tz'])}")
    notify("\n".join(lines))


def _all_landed(data, st):
    return all(st.get(fl["id"], {}).get("phase") == "LANDED" for fl in data["flights"])


def main():
    load_env()
    data = load_flights()
    st = load_state()
    apply_schedule_overrides(data, st)

    if "--status" in sys.argv:
        cmd_status(data, st)
        return

    if "--test" in sys.argv or "--test-telegram" in sys.argv:
        ch = os.environ.get("NOTIFY_CHANNEL") or ("whatsapp" if os.environ.get("WHATSAPP_APIKEY")
                                                  else "telegram" if os.environ.get("TELEGRAM_BOT_TOKEN") else "none")
        # Probe the real API rather than trusting that a key exists.
        adb_ok, delays = (aerodatabox_health(data["flights"][0]) if data.get("flights")
                          else (aerodatabox_configured(), "unknown (no flights configured)"))
        # Also log it: the verdict used to exist only inside the message sent to
        # the phone, so whoever ran the test could not see the result without
        # asking the traveller to read it back.
        log(f"   AeroDataBox health: {delays}")
        active = "⏰ reminder, 🛫 departure, 🛬 landing, en-route updates"
        if adb_ok:
            active += ", 🕒 delays, 🚪 gate changes, 🛑 cancellations"
        ok = notify("✅ <b>Flight monitor test</b> — alerts are wired up.\n"
                    f"Delay/gate/cancellation alerts: {delays}\n"
                    f"You'll get: {active}.",
                    verbose=True)
        print(f"Test alert via '{ch}':", "OK" if ok else "FAILED (see monitor.log / check secrets)")
        return

    announce_itinerary(data, st)

    from FlightRadar24 import FlightRadar24API
    api = FlightRadar24API()

    if "--once" in sys.argv:
        poll_cycle(api, data, st, datetime.now(timezone.utc))
        cmd_status(data, st)
        return

    if "--serve" in sys.argv:
        started = time.time()
        log(f"serve mode: polling every {SERVE_POLL_SEC}s for up to {SERVE_MAX_SEC}s "
            f"(delays={'on' if aerodatabox_configured() else 'off'})")
        while time.time() - started < SERVE_MAX_SEC:
            try:
                now = datetime.now(timezone.utc)
                if _all_landed(data, st):
                    log("All flights landed. Exiting serve early.")
                    return
                poll_cycle(api, data, st, now)
            except Exception:
                log("!! serve error:\n" + traceback.format_exc())
            time.sleep(SERVE_POLL_SEC)
        log("serve window elapsed; exiting (next job continues).")
        _mark_served_full_term()
        return

    # Plain continuous loop (local dev) — sends a one-time startup ping.
    if not st.get("_started"):
        lines = [f"👀 <b>Flight monitor live</b> — watching {len(data['flights'])} flights:"]
        for fl in data["flights"]:
            lines.append(f"• {fl['number']} {fl['leg']} — dep "
                         f"{local_and_home(fl['_dep_utc'], fl['origin_tz'], data['home_tz'])}")
        notify("\n".join(lines))
        st["_started"] = True
        save_state(st)
    log(f"Monitor started (loop). Watching {len(data['flights'])} flights.")
    while True:
        try:
            if _all_landed(data, st):
                notify("🏁 All flights complete. Safe travels!")
                return
            active = poll_cycle(api, data, st, datetime.now(timezone.utc))
            time.sleep(POLL_INTERVAL_SEC if active else IDLE_SLEEP_SEC)
        except KeyboardInterrupt:
            return
        except Exception:
            log("!! error:\n" + traceback.format_exc())
            time.sleep(60)


if __name__ == "__main__":
    main()
