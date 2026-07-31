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

BASE = Path(__file__).resolve().parent
FLIGHTS_FILE = BASE / "flights.json"
STATE_FILE = BASE / "state.json"
ENV_FILE = BASE / ".env"
LOG_FILE = BASE / "monitor.log"

# --- tunables ---------------------------------------------------------------
POLL_INTERVAL_SEC = 180          # cadence for the plain continuous loop (local)
SERVE_POLL_SEC = 60              # cadence in --serve mode (cloud, ~1 minute)
SERVE_MAX_SEC = 3300             # --serve runs ~55 min, then exits (next cron job continues)
IDLE_SLEEP_SEC = 300             # sleep when no window is open (local loop)

WINDOW_BEFORE_DEP_MIN = 45       # start position-watching this long before departure
WINDOW_AFTER_ARR_HRS = 4         # keep position-watching this long after arrival
PREDEP_REMINDER_MIN = 30         # "departs soon" reminder this long before departure
MISSING_POLLS_FOR_LANDING = 3    # consecutive "gone from feed" polls before a radar-drop can mean landed
MIN_ELAPSED_FRAC_FOR_RADAR_LAND = 0.85  # a radar-drop landing also requires >= this much of the flight elapsed
AIRBORNE_MIN_ALT_FT = 1000       # altitude above which we consider it truly flying

STATUS_BEFORE_DEP_HRS = 10       # start status-watching (delays/gate) this long before dep
STATUS_AFTER_ARR_HRS = 2         # keep status-watching this long after arrival
LANDING_NEAR_ETA_MIN = 45        # a radar dropout only counts as "landed" within this window of ETA
STATUS_POLL_SEC = 1200           # how often to hit AeroDataBox per flight (20 min → quota-friendly)
DELAY_ALERT_MIN = 15             # only alert departure delays >= this many minutes
DELAY_REALERT_MIN = 15           # re-alert when the delay changes by >= this many minutes
ARR_DELAY_ALERT_MIN = 20         # only alert arrival delays >= this many minutes

# Proactive "how's the trip going" updates while airborne (no question needed).
PROGRESS_FRACTIONS = [0.25, 0.50, 0.75]  # send an en-route update at these points of the flight
PROGRESS_MIN_DURATION_MIN = 180          # only send milestone updates for flights >= 3h
APPROACH_BEFORE_ARR_MIN = 45             # send an "on approach / descending" update this long before ETA
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
    return data


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
    channel = os.environ.get("NOTIFY_CHANNEL", "").strip().lower()
    have_wa = bool(os.environ.get("WHATSAPP_APIKEY") and os.environ.get("WHATSAPP_PHONE"))
    have_tg = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
    if not channel:
        channel = "whatsapp" if have_wa else ("telegram" if have_tg else "")
    if channel == "whatsapp":
        ok = send_whatsapp(text, verbose=verbose)
    elif channel == "telegram":
        ok = send_telegram(text)
    else:
        log("!! No notification channel configured. Message would have been:\n" + text)
        return False
    # Authoritative delivery record — independent of the caller's optimistic
    # ">> alert sent" line — so the logs always show whether an alert truly landed.
    if ok:
        log(f"   ✓ delivered via {channel}")
    else:
        log(f"   ✗ DELIVERY FAILED via {channel} — alert was NOT received (see error above)")
    return ok


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


def fetch_flight_status(fl):
    """Return a parsed status dict for this leg from AeroDataBox, or None."""
    key = os.environ.get("AERODATABOX_KEY")
    if not key:
        return None
    date = fl["sched_departure_local"].split(" ")[0]  # YYYY-MM-DD
    num = urllib.parse.quote(fl["number"])
    url = (f"https://aerodatabox.p.rapidapi.com/flights/number/{num}/{date}"
           f"?withAircraftImage=false&withLocation=false")
    req = urllib.request.Request(url, headers={
        "X-RapidAPI-Key": key,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
    })
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        log(f"   AeroDataBox error for {fl['number']}: {e}")
        return None
    items = data if isinstance(data, list) else (data.get("flights") if isinstance(data, dict) else None)
    if not items:
        return None
    chosen = None
    for it in items:
        dep_iata = ((it.get("departure") or {}).get("airport") or {}).get("iata")
        if dep_iata == fl["origin_iata"]:
            chosen = it
            break
    chosen = chosen or items[0]
    dep = chosen.get("departure") or {}
    arr = chosen.get("arrival") or {}
    return {
        "status": (chosen.get("status") or "").strip(),
        "dep_sched": _adb_time(dep.get("scheduledTime")),
        "dep_revised": _adb_time(dep.get("revisedTime") or dep.get("predictedTime") or dep.get("runwayTime")),
        "arr_sched": _adb_time(arr.get("scheduledTime")),
        "arr_revised": _adb_time(arr.get("revisedTime") or arr.get("predictedTime") or arr.get("runwayTime")),
        "dep_gate": dep.get("gate"),
        "dep_terminal": dep.get("terminal"),
        "arr_airport": (arr.get("airport") or {}).get("iata"),
    }


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

    info = fetch_flight_status(fl)
    s["last_check"] = now.isoformat()
    if not info:
        save_state(st)
        return

    header = f"✈️ <b>{fl['number']}</b> {fl['leg']}"
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

    header = f"✈️ <b>{fl['number']}</b> {fl['leg']}"

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
            process_progress(fl, s, st, header, home_tz, now, alt, spd)
        elif (on_ground and s["seen_airborne"] and _near_arrival(fl, st, now)
              and not _status_says_enroute(fl, st)):
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
            enroute = _status_says_enroute(fl, st)
            allow = (near and frac >= MIN_ELAPSED_FRAC_FOR_RADAR_LAND and not enroute)
            log(f"   {fl['number']}: not in feed ({s['missing_polls']}/{MISSING_POLLS_FOR_LANDING}) "
                f"near_arrival={near} elapsed={frac:.0%} enroute={enroute} allow_land={allow}")
            if allow and s["missing_polls"] >= MISSING_POLLS_FOR_LANDING:
                _fire_landing(fl, s, header, home_tz, now, "dropped off radar near destination")
        else:
            log(f"   {fl['number']}: not airborne yet / not in feed")


def _eta_and_note(fl, st):
    """Best-known arrival ETA (UTC) and an on-schedule note, using AeroDataBox if present."""
    ss = st.get(fl["id"] + "#status", {})
    eta = None
    if ss.get("eta_utc"):
        try:
            eta = datetime.fromisoformat(ss["eta_utc"])
        except Exception:
            eta = None
    if eta is None:
        eta = fl["_arr_utc"]
        return eta, "as scheduled"
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


def _status_says_enroute(fl, st):
    txt = (st.get(fl["id"] + "#status", {}).get("status_text") or "").lower()
    if not txt:
        return False  # no status info — fall back to the geometric guards
    if "arrived" in txt or "landed" in txt:
        return False
    return any(k in txt for k in _ENROUTE_STATUSES)


def _elapsed_frac(fl, s, st, now):
    try:
        departed = datetime.fromisoformat(s["departed_utc"]) if s.get("departed_utc") else fl["_dep_utc"]
    except Exception:
        departed = fl["_dep_utc"]
    eta, _ = _eta_and_note(fl, st)
    total = (eta - departed).total_seconds()
    return ((now - departed).total_seconds() / total) if total > 0 else 1.0


def _fire_landing(fl, s, header, home_tz, now, reason):
    if s["landed_alert"]:
        return
    s["phase"] = "LANDED"
    s["landed_alert"] = True
    s["landed_confirmed"] = True
    delta = round((now - fl["_arr_utc"]).total_seconds() / 60)
    punct = f"{abs(delta)} min early" if delta <= -5 else (f"{delta} min late" if delta >= 15 else "on time")
    notify(f"{header}\n\n🛬 <b>LANDED</b> at {fl['dest_name']}\n"
           f"Time: {local_and_home(now, fl['dest_tz'], home_tz)}\n"
           f"Scheduled: {local_and_home(fl['_arr_utc'], fl['dest_tz'], home_tz)} ({punct})")
    log(f"   >> LANDING alert sent for {fl['number']} ({reason})")


def poll_cycle(api, data, st, now):
    home_tz = data["home_tz"]
    any_active = False
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


def _all_landed(data, st):
    return all(st.get(fl["id"], {}).get("phase") == "LANDED" for fl in data["flights"])


def main():
    load_env()
    data = load_flights()
    st = load_state()

    if "--status" in sys.argv:
        cmd_status(data, st)
        return

    if "--test" in sys.argv or "--test-telegram" in sys.argv:
        ch = os.environ.get("NOTIFY_CHANNEL") or ("whatsapp" if os.environ.get("WHATSAPP_APIKEY")
                                                  else "telegram" if os.environ.get("TELEGRAM_BOT_TOKEN") else "none")
        delays = "on" if aerodatabox_configured() else "off (no AeroDataBox key yet)"
        ok = notify("✅ <b>Flight monitor test</b> — alerts are wired up.\n"
                    f"Delay/gate/cancellation alerts: {delays}.\n"
                    "You'll get ⏰ reminder, 🕒 delays, 🚪 gate changes, 🛫 departure, 🛬 landing.",
                    verbose=True)
        print(f"Test alert via '{ch}':", "OK" if ok else "FAILED (see monitor.log / check secrets)")
        return

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
        log("serve window elapsed; exiting (next scheduled job continues).")
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
