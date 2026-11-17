# Grafana dashboard: merged cluster metrics next to the stock exporter

This runs the same shop API twice, as four Node `cluster` workers each, with the same traffic going to both.

- `shop-api` uses this package. Workers report to the primary and the primary serves one `/metrics` for the whole cluster.
- `shop-api-stock` runs the stock OpenTelemetry Prometheus exporter inside each worker on the shared port. Node hands each scrape to whichever worker accepts the connection, so Prometheus sees one worker's numbers per scrape.

Every four minutes the primary retires its oldest worker and forks a new one. It chooses `SIGKILL` with 20% probability and `SIGTERM` otherwise. This is a synthetic workload, not production traffic.

The load generator independently counts attempted requests, received responses, transport failures and in-flight requests for each target. It serves these at `load:9100/metrics`. Received responses include HTTP 500s. It uses no OpenTelemetry instruments or cluster merge code.

```sh
cd examples/grafana
docker compose up
```

Then open http://localhost:3000/d/node-cluster-otel-metrics. Rates need at least two 15-second scrapes. Workers start together, so the comparison changes as they age and restart. There is no fixed inflation factor. Keep it running through several replacements to see that effect.

`shop-api` keeps its totals in a file (`STATE_FILE`, on the `shop-api-state` volume). Run `docker compose restart shop-api shop-api-stock` and blue's raw count carries on, while the usual setup starts again from zero.

## The live copy on Grafana Cloud

The live dashboard is at https://brightparlor286.grafana.net/public-dashboards/77669c3724254e73b71b22f737f34744. It runs `cloud/compose.yaml`, which differs from the local setup in three ways:

- `shop-api` pushes its one merged count over OTLP every 15 seconds to an OpenTelemetry Collector. Nothing scrapes it. The Collector scrapes `shop-api-stock` and the load generator, then sends everything to Grafana Cloud.
- `shop-api` keeps its totals in a file, and both main programs restart every day at 15:00 UTC (`docker compose restart shop-api shop-api-stock` from a timer). Blue's raw count carries on across the restart. The usual setup starts again from zero.
- The Collector drops the unit from pushed metrics. With a unit, OTLP metrics get a unit suffix in Prometheus names (`worker_heap_used_bytes`), and both shops would need different queries.

To run it against your own stack, copy `cloud/ingest.env.example` to `cloud/ingest.env`, fill in your OTLP endpoint and header, then `docker compose -f cloud/compose.yaml up -d --build`.

The dashboard is also on grafana.com as [26076](https://grafana.com/grafana/dashboards/26076), so you can import it with Dashboards > New > Import and that ID. [docs/dashboard.mp4](../../docs/dashboard.mp4) is a 30-second video of the live copy, with a restart of both main programs in it.

## The panels


The dashboard opens on one question: is my shop as busy as my graph says? Blue is "My package" (`shop-api`), orange is "Usual setup" (`shop-api-stock`), and the dashed green line is "What visitors got back", responses independently received from `shop-api` by the load generator.

- **Usual setup vs what visitors got.** Stock-exporter request rate divided by responses received from that shop, over one hour. A comparison near 1x is expected with correct counting. This denominator is independent of the package under test.
- **Visitors a second, counted three ways.** Both clusters get the same request attempts. Drops within a stock counter series are read as resets by `rate()`. A faint violet bar marks a new worker ID on the merged side. It does not identify whether the old worker left through SIGTERM or SIGKILL. The primary logs the actual signal.
- **Visitors the usual setup made up, last 24 hours.** Stock-exporter request increase minus the increase in responses received from that shop, over 24 hours.
- **Count resets, last hour.** Sum of per-series `resets()`, across route and status labels. This is not the number of process restarts. Merged worker totals survive worker exits, and with `STATE_FILE` set they survive a primary restart too.
- **Copies the graph can see (the shop runs four).** Both clusters run four workers. Every merged report has all four. The stock exporter only ever shows the one worker that took the scrape. The panel counts the copies in the newest report only, because pushed series have no staleness markers, so a retired worker's last value would otherwise stay visible for the backend's lookback (5 minutes by default).
- **My package vs what visitors got.** Merged request rate divided by responses independently received from the merged shop. A SIGTERM handler sends final worker totals before exit. SIGKILL can lose increments since the last snapshot, normally up to the default 10-second push interval. Transport failures and responses still in flight can also separate client and server counts. This comparison is not a lossless accounting guarantee.
- **Try it on your app.** The same check as the green line, as one command for your own app: `npx @adityareddy-dev/otel-cluster-metrics check`. See [Check your request graph](../../README.md#check-your-request-graph).

The collapsed rows below hold the raw counter next to the responses received (green), each setup's own count, p95 latency, and the merged cluster's routes, errors, in-flight requests, email queue (a gauge summed across workers with `gaugeReducers: { 'queue.depth': 'sum' }`) and V8 heap per worker. Gauges stay per worker, keyed by `cluster.worker.id` (worker ID plus boot ID), so a retired worker's heap line ends and its replacement starts a new one.

The merged details include client transport failures per target. A response lost after the server records it can appear in telemetry without appearing in the client's received-response count.

`dashboard.json` has a Prometheus data source picker, so it also imports through Dashboards > New > Import. It needs no additional data source plugin. The metric names are the ones the OpenTelemetry Prometheus exporter writes (`shop_requests_total`, `shop_request_duration_bucket` and so on).

For a repeatable correctness check from the repository root, run:

```sh
npm ci
npm test
node examples/single-task-workers.cjs
```

The single-task example compares 12 independently reported task results with the exported counter after all workers exit. The tests also cover graceful finals, hard-kill snapshot loss, atomic HTTP failure/recovery and randomized worker lifecycles. These checks establish the package's tested behavior. The dashboard's client comparison shows what the running synthetic workload is observing.
