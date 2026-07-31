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
executes `python monitor.py --serve`, polling **every ~60 seconds** for ~5h45m —
just under GitHub's 6h job cap, because every handoff between jobs is a chance to
fall in a hole, so fewer per day is better (4, not 8).

Coverage is continuous only if a successor starts when a job ends. Two
independent mechanisms drive that:

1. **Self-handoff (deterministic).** A run that served its full window starts its
   own successor before exiting. This is the reliable path, and it is **optional**
   — see below.
2. **`*/10` cron (backstop).** The `flight-monitor` concurrency group keeps
   exactly one job alive, so a tick landing mid-run just queues and takes over the
   instant the running job exits. Ticks are superseded by later ones, which is why
   the Actions tab shows a steady stream of **cancelled** runs — that is the
   mechanism working, not failing.

The cron alone is not enough: GitHub's scheduler is best-effort and was observed
**dropping ~90% of ticks** on this repo, once leaving a 43-minute hole between
jobs. When a tick *is* queued the takeover is instant (measured: 4 seconds).

### Enabling guaranteed handoff (optional)

Add a `HANDOFF_PAT` secret — a fine-grained PAT for this repo with
**Actions: read and write**. The built-in `GITHUB_TOKEN` cannot be used: GitHub
deliberately bars it from triggering workflows.

Without the secret the handoff step is a no-op and the cron backstop applies.
A run only hands off if it wrote `.served-full-term`, so a run that crashed on
startup — or exited because every flight has landed — ends the chain instead of
spinning a hot loop.

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
