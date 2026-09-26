# Market Playground reset plan

## Product direction

Treat this first as a personal SPX market-research tool. The useful question is not
"what features can this app have?" but "what can I learn from the snapshots I have
already collected that I cannot answer quickly today?"

The strongest initial direction is a fast historical market replay: choose a day or
date range, see the SPX and volatility context, inspect the options snapshots that
exist for that period, and then compare a clearly described setup across recorded
sessions. Keep strategy analysis as one research view, not the product's organizing
principle. Clearly label missing observations and estimates; this dataset is not a
source of executable fills.

## Sequence

1. **Make the existing research loop responsive.** Start strategy runs with a
   bounded recent range, measure the server query, payload, browser processing, and
   render time separately, then optimize the measured bottleneck. Keep longer
   history available as an explicit choice.
2. **Make local review the only pre-merge environment.** Run the production app
   locally against a local database, inspect it in a browser, and merge reviewed
   changes to `main`. Do not maintain a separate staging server or copy changes
   between dev, staging, and production implementations.
3. **Reduce the product to one primary workflow.** Center the app on market replay
   and historical setup comparison. Move SQL diagnostics and technical metadata
   out of the main experience. Keep collection and backup operations separate from
   research UI.
4. **Replace the interface in slices.** Start with a clean overview and date
   selection, then add market context, option history, and finally strategy
   comparison. Keep the current production service and database contract working
   until each replacement slice is useful.
5. **Keep only high-value verification.** Retain focused checks for data
   collection, date/contract resolution, historical calculations, sharing, and
   backup integrity. Remove redundant coverage as duplicated apps are retired; do
   not treat test count as a quality goal.
6. **Choose follow-on use cases from the data and actual use.** Candidate views
   include intraday regime replay, option premium/greeks behavior around SPX moves,
   setup outcome distributions, and data coverage/quality. Confirm that the stored
   history supports a view before making it a core feature.

## Deployment loop

Use the same production implementation locally and on the server:

```bash
source .venv/bin/activate
PYTHONPATH=src python -m spx_collector.backtest_prod --host 127.0.0.1 --port 8789
```

Point local `DB_URL` at a local copy of the SQLite data. Review the page and the
changed workflow locally, then merge the feature branch to `main`. Production stays
on its existing service and data; deployment remains a separate fast-forward pull
and service restart after merge.

## First change

Strategy analysis previously defaulted to every recorded session. It now starts
with the latest 20 sessions while leaving the date controls available for broader
analysis. This reduces default work without hiding historical data. The next
performance change should follow a measured run against a representative database;
there is no local snapshot database in this checkout to establish honest timing
baselines.
