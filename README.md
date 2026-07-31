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

You can also **ask** the bot at any time (Telegram):

| Command | Reply |
|---------|-------|
| `/status` | phase, scheduled/ETA times and time remaining for every leg |
| `/where` | same, plus a live altitude / speed fix |
| `/help` | command list |

Only the configured `TELEGRAM_CHAT_ID` gets answers — a bot is discoverable by
its `@username`, so itinerary details never go to a stranger.

## How it runs

`.github/workflows/monitor.yml` runs **continuously**, not in windows. Each job
executes `python monitor.py --serve`, polling **every ~60 seconds** for ~2h55m.
A `*/10` cron acts as a handoff/watchdog tick: the `flight-monitor` concurrency
group keeps exactly one job alive, so a new run simply waits and starts the
instant the previous one exits — no boundary to fall through — and if a job ever
dies the next tick revives it.

The cron is far denser than one handoff every ~3h needs, on purpose: GitHub's
scheduler is best-effort and **silently drops ticks** under load, so each tick is
another chance to queue the successor. Ticks that arrive mid-run just queue and
are superseded by the next one, which is why the Actions tab shows a steady
stream of **cancelled** runs — that is the mechanism working, not failing.

Runtime state lives in the **Actions cache**, so alerts fire once and nothing
personal is committed to this public repo.

FlightRadar24 is only queried while a flight is inside its watch window; outside
that the loop just checks for inbound commands, so running 24/7 costs no extra
flight-data requests. Actions minutes are unlimited on public repos.

This repo is **public** only so Actions minutes are unlimited (enabling the ~1-min
cadence for free). All personal data and credentials are **encrypted repo secrets**:

| Secret | What |
|--------|------|
| `FLIGHTS_JSON` | The real itinerary (passenger, booking, flights). Schema: see `flights.example.json`. |
| `WHATSAPP_PHONE` / `WHATSAPP_APIKEY` | CallMeBot WhatsApp delivery. |
| `AERODATABOX_KEY` | RapidAPI key for delay/gate/cancellation data (optional — omit and those alerts are skipped). |

## Changing flights or timing

- **Flights:** edit the `FLIGHTS_JSON` secret (Settings → Secrets → Actions). Shape in `flights.example.json`.
- **When it runs:** edit the `schedule:` cron in `monitor.yml` (UTC). To go back to
  windowed runs, narrow the cron and shorten `SERVE_MAX_SEC` in the workflow.
- **Channel:** set the `NOTIFY_CHANNEL` repo **variable** to `whatsapp` or `telegram`.
  Whichever is not primary is used as an automatic fallback if configured, so a
  throttled or failing channel can't silently swallow an alert.
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
  possible from any source. In-loop cadence is ~60s. Job handoffs are seamless,
  but GitHub's scheduler can still lag a few minutes when reviving a dead job.
- The loop exits early once every flight has landed, so after the last leg the
  bot stops answering commands until the next trip is configured.
- FlightRadar24's free feed only shows airborne aircraft, so departure/landing are
  position-based; landing is confirmed when the flight leaves the live feed (~6–10
  min after touchdown). Delay/gate/cancellation come from AeroDataBox instead.
- Datacenter IPs are very occasionally rate-limited by FR24; it self-corrects next poll.
