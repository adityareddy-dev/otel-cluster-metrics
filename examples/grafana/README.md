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

The dashboard opens on one question: is my shop as busy as my graph says? "Blue" is `shop-api`, "orange" is `shop-api-stock`.

- **The usual setup reports.** Requests a second from the stock exporter divided by the real count from the merged endpoint, over the last hour. It should be 1x; the stock exporter usually reports four to five times the real traffic.
- **Requests a second.** Same visitors to both, so the lines should match. Every drop in the stock counter is read as a reset, which inflates `rate()`. A violet bar marks each worker restart, read from the merged endpoint: a `cluster.worker.id` that wasn't there one step earlier. Hover it to see which worker started.
- **Times the count started over, last hour.** Zero for merged. The stock exporter racks up hundreds an hour, at each restart and each time a different worker answers the scrape.
- **Workers the graph can see.** Both clusters run four workers. The merged endpoint reports all four on every scrape; the stock exporter only ever shows the one worker that took the scrape.
- **npm downloads per week.** Weekly downloads of this package from the npm registry, read through the [Infinity](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/) data source. Compose installs the plugin and provisions the data source.

The collapsed rows below hold the raw counter, each setup's own count, p95 latency, and the merged cluster's routes, errors, in-flight requests, email queue (a gauge summed across workers with `gaugeReducers: { 'queue.depth': 'sum' }`) and V8 heap per worker. Gauges stay per worker, keyed by `cluster.worker.id` (worker ID plus boot ID), so a retired worker's heap line ends and its replacement starts a new one.

`dashboard.json` has Prometheus and Infinity data source pickers, so it also imports into any Grafana through Dashboards > New > Import. The metric names are the ones the OpenTelemetry Prometheus exporter writes (`shop_requests_total`, `shop_request_duration_bucket` and so on), and scraping through an OpenTelemetry Collector into Grafana Cloud keeps the same names.
