# Flight Monitor 🛫🛬

Near-real-time flight alerts over WhatsApp, running entirely in GitHub Actions
(no server, no always-on laptop). Watches a set of flights and pings you on:

| Alert | Source |
|-------|--------|
| ⏰ ~30 min before departure | schedule |
| 🕒 delay / new ETA | AeroDataBox |
| 🚪 gate / terminal change | AeroDataBox |
| 🛑 cancellation / diversion | AeroDataBox |
| 🛫 departed (airborne) | FlightRadar24 live feed |
| 🛬 landed | FlightRadar24 live feed |

## How it runs

`.github/workflows/monitor.yml` triggers hourly during the travel windows. Each
run executes `python monitor.py --serve`, which polls **every ~60 seconds** for
~55 minutes; the next hourly job continues. Runtime state lives in the **Actions
cache**, so alerts fire once and nothing personal is committed to this public repo.

This repo is **public** only so Actions minutes are unlimited (enabling the ~1-min
cadence for free). All personal data and credentials are **encrypted repo secrets**:

| Secret | What |
|--------|------|
| `FLIGHTS_JSON` | The real itinerary (passenger, booking, flights). Schema: see `flights.example.json`. |
| `WHATSAPP_PHONE` / `WHATSAPP_APIKEY` | CallMeBot WhatsApp delivery. |
| `AERODATABOX_KEY` | RapidAPI key for delay/gate/cancellation data (optional — omit and those alerts are skipped). |

## Changing flights or timing

- **Flights:** edit the `FLIGHTS_JSON` secret (Settings → Secrets → Actions). Shape in `flights.example.json`.
- **When it runs:** edit the `schedule:` crons in `monitor.yml` (UTC).
- **Tuning:** thresholds and cadences are constants at the top of `monitor.py`.

## Run it manually

Actions tab → *flight-monitor* → **Run workflow** → pick `test` (send a test
WhatsApp), `once` (one poll cycle), or `serve` (a full ~55-min live loop).

## Local use (optional)

```bash
pip install -r requirements.txt
cp flights.example.json flights.json   # edit with real flights
cp .env.example .env                   # add WhatsApp (+ optional AeroDataBox) creds
python3 monitor.py --test              # test alert
python3 monitor.py --serve             # ~55-min live loop
python3 monitor.py --status            # show state
```

## Honest limits

- Flight data providers update every minute or two — "live to the second" isn't
  possible from any source. In-loop cadence is ~60s; there's a ~5-min gap at each
  hourly job boundary. GitHub's scheduler can also occasionally lag a few minutes.
- FlightRadar24's free feed only shows airborne aircraft, so departure/landing are
  position-based; landing is confirmed when the flight leaves the live feed (~6–10
  min after touchdown). Delay/gate/cancellation come from AeroDataBox instead.
- Datacenter IPs are very occasionally rate-limited by FR24; it self-corrects next poll.
