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

The existing Grafana Cloud dashboard is at https://brightparlor286.grafana.net/public-dashboards/77669c3724254e73b71b22f737f34744. Deploying this revision there also requires scraping the load generator. The local Compose setup provisions all three scrape jobs.

The dashboard opens on one question: is my shop as busy as my graph says? Blue is `shop-api`, orange is `shop-api-stock`, and the dashed green line is responses independently received from `shop-api` by the load generator.

- **Stock / client responses.** Stock-exporter request rate divided by responses received from that shop, over one hour. A comparison near 1x is expected with correct counting. This denominator is independent of the package under test.
- **Requests a second.** Both clusters get the same request attempts. Drops within a stock counter series are read as resets by `rate()`. A violet bar marks a new worker ID on the merged side. It does not identify whether the old worker left through SIGTERM or SIGKILL. The primary logs the actual signal.
- **Counter decreases across all series, last hour.** Sum of per-series `resets()`, across route and status labels. This is not the number of process restarts. Merged worker totals survive worker exits, but restart when the primary restarts.
- **Workers the graph can see.** Both clusters run four workers. The merged endpoint reports all four on every scrape; the stock exporter only ever shows the one worker that took the scrape.
- **Merged / client responses.** Merged request rate divided by responses independently received from the merged shop. A SIGTERM handler sends final worker totals before exit. SIGKILL can lose increments since the last snapshot, normally up to the default 10-second push interval. Transport failures and responses still in flight can also separate client and server counts. This comparison is not a lossless accounting guarantee.

The collapsed rows below hold the raw counter, each setup's own count, p95 latency, and the merged cluster's routes, errors, in-flight requests, email queue (a gauge summed across workers with `gaugeReducers: { 'queue.depth': 'sum' }`) and V8 heap per worker. Gauges stay per worker, keyed by `cluster.worker.id` (worker ID plus boot ID), so a retired worker's heap line ends and its replacement starts a new one.

The merged details include client transport failures per target. A response lost after the server records it can appear in telemetry without appearing in the client's received-response count.

`dashboard.json` has a Prometheus data source picker, so it also imports through Dashboards > New > Import. It needs no additional data source plugin. The metric names are the ones the OpenTelemetry Prometheus exporter writes (`shop_requests_total`, `shop_request_duration_bucket` and so on).

For a repeatable correctness check from the repository root, run:

```sh
npm ci
npm test
node examples/single-task-workers.cjs
```

The single-task example compares 12 independently reported task results with the exported counter after all workers exit. The tests also cover graceful finals, hard-kill snapshot loss, atomic HTTP failure/recovery and randomized worker lifecycles. These checks establish the package's tested behavior. The dashboard's client comparison shows what the running synthetic workload is observing.
