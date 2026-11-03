# Grafana dashboard: merged cluster metrics next to the stock exporter

This runs the same shop API twice, as four Node `cluster` workers each, with the same traffic going to both.

- `shop-api` uses this package. Workers report to the primary and the primary serves one `/metrics` for the whole cluster.
- `shop-api-stock` runs the stock OpenTelemetry Prometheus exporter inside each worker on the shared port. Node hands each scrape to whichever worker accepts the connection, so Prometheus sees one worker's numbers per scrape.

Every four minutes the primary retires its oldest worker and forks a new one, one in five times with `SIGKILL`, so restarts show up on the dashboard too.

```sh
cd examples/grafana
docker compose up
```

Then open http://localhost:3000/d/node-cluster-otel-metrics. Give it a few minutes to build up history.

The same setup runs on Grafana Cloud, scraped through an OpenTelemetry Collector: https://brightparlor286.grafana.net/public-dashboards/77669c3724254e73b71b22f737f34744

What to look at:

- **Requests counter as Prometheus sees it.** The merged counter climbs in a straight line. The stock one jumps between workers' counts and falls back every time a worker restarts.
- **Resets in 1h.** Zero for merged. The stock exporter racks up dozens an hour, and each one makes `rate()` and `increase()` treat the drop as a restart. On the requests per second panel that inflates the stock line to several times the real traffic.
- **V8 heap per worker.** Gauges stay per worker, keyed by `cluster.worker.id` (worker ID plus boot ID), so a retired worker's line ends and its replacement starts a new one.
- **Email queue.** A gauge summed across workers with `gaugeReducers: { 'queue.depth': 'sum' }`.

`dashboard.json` has a Prometheus data source picker, so it also imports into any Grafana through Dashboards > New > Import. The metric names are the ones the OpenTelemetry Prometheus exporter writes (`shop_requests_total`, `shop_request_duration_bucket` and so on), and scraping through an OpenTelemetry Collector into Grafana Cloud keeps the same names.
