# otel-cluster-metrics

Collect cumulative OpenTelemetry metrics from Node `cluster` workers through one Prometheus endpoint in the primary process.

This independent experimental package supports one host and one explicitly configured service resource. It is not an official OpenTelemetry component. Integration uses the experimental `metricProducers` hook with the OpenTelemetry version ranges below.

```sh
npm install @adityareddy-dev/otel-cluster-metrics \
  '@opentelemetry/api@>=1.10.0 <1.11.0' '@opentelemetry/sdk-metrics@^3.0.0' \
  '@opentelemetry/exporter-prometheus@>=0.300.0 <0.302.0' \
  '@opentelemetry/resources@^3.0.0' '@opentelemetry/core@^3.0.0'
```

TypeScript applications also need `@types/node` in their development dependencies.

Node 22.15.0 or newer is required. Supported peers are API 1.10.x, SDK metrics/resources/core 3.x and Prometheus exporter 0.300.x or 0.301.x. CI tests Node 22 and 24 with both the lowest versions (API 1.10.0, SDK metrics/resources/core 3.0.0 and Prometheus exporter 0.300.0) and the latest versions in these ranges.

## Worker

Register the reader before recording metrics. Workers do not start exporters.

```js
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { WorkerMetricReader } = require('@adityareddy-dev/otel-cluster-metrics');

const resource = resourceFromAttributes({ 'service.name': 'my-service' });
const reader = new WorkerMetricReader();
const provider = new MeterProvider({ resource, readers: [reader], sdkMetricsEnabled: false });
const meter = provider.getMeter('my-service', '1.0');
meter.createCounter('requests').add(1);
// Signal readiness to the primary after installing the reader and instruments.
process.send({ ready: true });
// Hand the final totals to the primary before leaving. worker.kill() sends SIGTERM with the channel open.
process.on('SIGTERM', async () => { await provider.shutdown(); process.exit(0); });
```

`provider.shutdown()` sends the worker's final totals to the primary and resolves after the message is written, so call it before `process.exit()`. `provider.forceFlush()` pushes a snapshot now. Every `pushIntervalMillis` (default 10000) the reader pushes a snapshot on its own, which bounds what a crash loses. See Worker lifecycle and loss windows.

## Primary

Wait until the workers have signalled readiness before accepting scrapes. Supply the same common resource to the producer and primary provider. Keep application instruments out of this primary provider, and disable SDK self-observability metrics to avoid adding primary streams.

```js
const { createServer } = require('node:http');
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { ClusterMetricProducer, createPrometheusHandler } = require('@adityareddy-dev/otel-cluster-metrics');

const resource = resourceFromAttributes({ 'service.name': 'my-service' });
const producer = new ClusterMetricProducer({ resource });
const exporter = new PrometheusExporter({
  preventServerStart: true,
  metricProducers: [producer],
});
const provider = new MeterProvider({ resource, readers: [exporter], sdkMetricsEnabled: false });
const handler = createPrometheusHandler(exporter);
const server = createServer((req, res) => {
  if (req.url === '/metrics') void handler(req, res);
  else { res.writeHead(404); res.end(); }
});
server.listen(9464);
// At shutdown: close server, await provider.shutdown(), await producer.shutdown().
```

Use `createPrometheusHandler` with `preventServerStart: true`. The stock exporter HTTP server returns 200 even when its collection rejects. This handler calls the exporter's public `collect()` method and serializer, returns 503 for any incomplete round, and exposes no partial metrics on failure. If configuring serializer options such as a prefix, pass an explicitly configured `PrometheusSerializer` as the handler's second argument. The helper does not copy exporter serializer settings automatically.

## Push over OTLP instead

The producer works with any metric reader, so the primary can push the merged totals over OTLP, for example straight to Grafana Cloud, with no Prometheus scrape in between. Use a `PeriodicExportingMetricReader` in place of the Prometheus exporter and install `@opentelemetry/exporter-metrics-otlp-http` next to the other OpenTelemetry packages:

```sh
npm install '@opentelemetry/exporter-metrics-otlp-http@>=0.300.0 <0.302.0'
```

CI tests the OTLP HTTP exporter at 0.300.0 and the latest version in this range.

```js
const { MeterProvider, PeriodicExportingMetricReader } = require('@opentelemetry/sdk-metrics');
const { OTLPMetricExporter } = require('@opentelemetry/exporter-metrics-otlp-http');

const producer = new ClusterMetricProducer({ resource });
const reader = new PeriodicExportingMetricReader({
  exporter: new OTLPMetricExporter(),
  exportIntervalMillis: 60000,
  metricProducers: [producer],
});
const provider = new MeterProvider({ resource, readers: [reader], sdkMetricsEnabled: false });
```

The exporter reads `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS`. A round that misses a worker exports nothing for that interval, so the backend never gets a partial total, and the next interval sends the full one. The reader does not throw when an export fails. It logs through the OpenTelemetry `diag` logger, so set one up or check the exporter result if you need to know. `examples/otlp-push.cjs` shows both: it runs two workers against a local receiver, or against your endpoint when those variables are set, and exits non-zero if the export fails.

## Check your request graph

Use a quiet test or staging app. Pick a GET route and a counter that adds one per request, including errors, but excludes `/metrics`. Copy the counter name from that endpoint, with or without `_total`:

```sh
npx @adityareddy-dev/otel-cluster-metrics check \
  --target http://127.0.0.1:3000/ \
  --metrics http://127.0.0.1:9464/metrics \
  --counter requests_total --duration 3 --rate 50 --scrape 0.3
```

Output from a real four-worker run of `test/check-app.cjs` in merged mode included:

```text
Sent 149 requests. Received 149 complete responses (0 HTTP 4xx/5xx). Failed 0 requests.
Sampled counter increase: 149 from requests_total (11 scrapes, 0 counter drops).
Counter / received responses: 1.00x what the app really answered.
```

The stock exporter returned 38 increments for 149 responses in the same fixture, or 0.26x.

`Sent` includes every attempt. A response counts only after its whole body arrives, with HTTP errors listed separately. Timeouts and broken connections count as failed requests and leave the ratio unknown. Redirects count without being followed.

The check reads a baseline, then adds the changes in each counter series. A drop adds the new value as a reset. It uses fresh connections, so workers sharing a metrics port can produce an overcount or undercount. A dashboard with different scrapes or PromQL may differ.

Defaults are 60 seconds, 20 requests a second and 5 seconds between reads. Allow one more interval after the last response. `--timeout` sets each request's deadline in seconds (default 5).

Exit codes: 0 for a match within 2% with no request errors, 1 for a mismatch or unknown ratio, 2 for bad input or an unreadable counter.

## Merge behavior

| Emitted data | Behavior |
| --- | --- |
| Monotonic Sum | Add one latest cumulative snapshot per worker epoch. Retain exited workers' totals. |
| Non-monotonic Sum | Add current worker-local values. Remove exited workers. |
| Explicit Histogram | Add count, sum and corresponding bucket counts. Boundaries must match. Keep min/max only when every nonempty population supplies them. Retain exited workers. |
| Gauge | Separate series with `cluster.worker.id` containing worker ID and boot ID. Remove exited workers. |

Gauges may be combined explicitly by metric name:

```js
const producer = new ClusterMetricProducer({
  resource,
  gaugeReducers: { queue_depth: 'sum', temperature: 'mean' },
});
```

Available reducers are `sum`, `min`, `max`, and `mean`. Mean weights worker series equally. The setting applies to that metric name in every scope. Attributes still partition series. Do not supply the reserved `cluster.worker.id` attribute on gauges that use the default separate-series behavior.

Worker metrics must use the configured common resource, including resource schema URL. If resources include per-process identity, remove only explicitly chosen attributes:

```js
new ClusterMetricProducer({
  resource,
  omitResourceAttributes: ['process.pid', 'service.instance.id'],
});
```

The exclusions affect comparison in both resources. Output always uses the supplied primary resource, so omit worker-specific attributes from that common resource too. All remaining resource differences fail the round. The exporter hook ignores producer resources and substitutes its own provider resource, which is why both must match.

Series identity includes scope name/version/schema, descriptor name/unit/value type/point type/monotonicity/temporality and sorted, typed attributes. Conflicting descriptors and histogram boundaries fail the round. Description differences emit an OTel diagnostic and choose the lexically smallest description in the contributing data.

Cumulative series disappearing from a live worker remain in that worker's last snapshot until they reappear or the worker exits. A reappearance with the same start time replaces the old value. A changed point start time retires the old cumulative total and starts a new epoch. Gauges and non-monotonic sums disappear immediately when absent. Retired counter and histogram epochs fold into one total per series. Retired gauges are never stored. Decreasing monotonic values without a changed start time fail the round.

The aggregate has one start timestamp established when the producer is constructed and a fresh end timestamp for each successful round. A primary restart resets aggregate totals unless `stateFile` is configured, as described below. A worker crash loses what it recorded after its last push, flush or reply, see Worker lifecycle and loss windows. Instruments in multiple workers must measure worker-local work. Reporting the same shared external total from every worker would double-count.

## Keep totals across primary restarts

Set `stateFile` to keep totals when the primary and its workers restart:

```js
const producer = new ClusterMetricProducer({
  resource,
  stateFile: './cluster-metrics.json',
  onDiagnostic: message => console.warn(message),
});
```

The path is resolved at construction. Create its parent directory first, make it writable, and use a local filesystem with one file per primary.

Monotonic sums, explicit histograms and their aggregate start timestamp survive. The file includes live workers' last committed snapshots and exited workers' totals. New workers add to these totals. Gauges and non-monotonic sums start fresh. Restart the workers with the primary, otherwise their cumulative snapshots would be counted again.

Writes are synchronous, after successful rounds, accepted pushes and finals, worker departures and `producer.shutdown()`. Each write flushes a temporary file with `fsync` and renames it over the old file. Failed rounds never save their replies. At shutdown, stop the workers through their providers first, then shut down the primary provider and producer.

A missing file starts fresh. Corrupt or unreadable files, unsupported format versions and configuration mismatches throw at construction and report through `onDiagnostic`. The resource, including its schema URL, resource exclusions, gauge reducers, timeout and limits must match. Attribute, exclusion and reducer ordering does not matter. Restored totals count toward `maxSeries` alongside new workers. To start fresh, move the file aside or choose another path.

An exclusive `.lock` file protects each write. If another producer changes the saved contents, this producer reports a diagnostic and stops saving for its remaining lifetime. A busy lock or write failure leaves the previous file intact and retries on the next save. Collection still works, so monitor `onDiagnostic`.

A crash loses work since the last successful save and anything workers have not reported. A crash during a write can leave a lock and temporary file. After checking that no writer is running, remove those leftovers so saves can resume. The last complete state file can still be restored. Recovery after a filesystem or power failure is not guaranteed.

## Worker lifecycle and loss windows

The primary learns a worker's cumulative state three ways: the reply to a scrape, an unrequested push, and a final snapshot. It also watches the cluster `disconnect` and `exit` events.

- Push. Every `pushIntervalMillis` the worker collects and sends its state. The default is 10000 ms and on, because one collection and one IPC message per worker every 10 s costs less than a scrape and bounds what a crash loses to 10 s instead of the scrape interval. The timer is unreferenced, so it never keeps a finished worker alive. Set 0 to turn the push off; a crash then loses everything since the last scrape reply or flush.
- Final. `provider.shutdown()` collects once more and sends the result marked final. The primary folds it into the retained totals at once and stops asking that worker. `beforeExit` sends the same final, but a cluster worker never reaches `beforeExit` while its IPC channel is open, so only an application that calls `process.channel.unref()` gets that path.
- Exit and disconnect. On the cluster `disconnect` or `exit` event the primary retires the worker's last known snapshot, whether final, push or reply, into the totals. The primary cannot ask a leaving worker for anything: by the time either event fires the channel is closed on both sides, so the last snapshot is all there is.

A worker that starts, records and exits between two scrapes keeps its increments when it shuts down through the provider, or when the push timer fired before it left. Pushes and finals that arrive during a round are held and applied after the round commits or fails, so a round stays atomic. A final arriving during a round serves as that worker's reply, so a worker that shuts down mid-scrape does not fail the scrape. A worker that exits mid-scrape without a final does fail that round, and the next round proceeds without it.

What is still lost:

| How the worker ends | Lost |
| --- | --- |
| `provider.shutdown()` awaited before exit, including a SIGTERM handler that does so | Nothing |
| SIGTERM with no handler, `process.exit()` without shutdown, `worker.disconnect()` from the primary | Increments since the last push, flush or reply, at most one push interval |
| SIGKILL, OOM kill, an uncaught crash | The same window, at most one push interval |
| A primary restart without `stateFile` | Every total, since the primary holds the state |
| A primary restart with `stateFile` | Work since the last successful save and work workers have not reported |

## Per-worker counters

Counters and histograms carry no per-worker attribute, and there is no option to add one. Every exited worker would leave one series per metric behind for the life of the primary, and a deployment that forks a worker per task would grow that count without bound. The sum is also what `rate()` and `increase()` need. To watch one worker's number, keep the count in a worker-local variable and report it through an observable gauge. Gauges carry `cluster.worker.id` and disappear when the worker exits, so the series count stays equal to the live worker count. Such a gauge restarts from zero with each worker and is not a counter for Prometheus.

## Single-task workers

A worker forked for one task and exiting when it is done should report the task's result in band, with `process.send`, because that is what the primary acts on. Its metrics still reach the primary when it awaits `provider.shutdown()` before exiting. `examples/single-task-workers.cjs` forks three such workers, sums their in-band results, scrapes after every worker has exited, and checks that both say 12.

## Collection and limits

Concurrent scrapes share one collection round. Each round snapshots the worker membership, requests one collection per worker, and commits state only after every response validates. A missing or disconnected worker, a worker exiting during a round, an invalid response or a collection error fails that round. A failed round leaves previously committed state intact. The next round retries current workers. An empty cluster retains retired totals and has no live gauges.

IPC uses a versioned JSON string envelope with request ID, boot ID, sequence, request timestamp and collection timestamp. Resources and metric data are explicitly encoded. No SDK class instances cross IPC. Non-finite numeric gauges and attributes use tagged numbers rather than JSON nulls. Duplicate and late replies are discarded. Sequence and point timestamp checks reject stale snapshots. Use `onDiagnostic(message)` to observe duplicate or late responses received during a round, and pushes or finals that were discarded because they did not reconcile with the worker's earlier snapshot.

Defaults:

| Option | Default | Meaning |
| --- | --- | --- |
| `timeoutMillis` | 5000 | Primary round deadline in milliseconds. A shorter `collect({timeoutMillis})` wins for a new round. |
| `maxMessageBytes` | 1048576 | Maximum bytes of a worker IPC envelope, configurable on both classes. |
| `maxSeries` | 10000 | Primary retained entries across active worker snapshots and retired totals, also caps points/descriptors per worker reply. |
| `maxWorkers` | 64 | Maximum current workers. |
| `stateFile` | Off | Primary file for cumulative counters, explicit histograms and the aggregate start timestamp. |
| `maxPoints` | 10000 | Worker reader point/descriptor limit. |
| `pushIntervalMillis` | 10000 | Worker reader. Milliseconds between unrequested snapshots to the primary. 0 turns the push off. |

Limits fail collection instead of evicting cumulative totals. Repeated worker exits do not retain a list of old epochs: there is one retained total per series however many workers have come and gone. A push or final that would breach a limit is discarded with a diagnostic and the committed state stays as it was. Changing attribute sets can still reach the series limit and require a fresh state file, a primary restart without persistence, or a larger explicit limit with a fresh file. Tune limits on both sides together. The SDK also applies its own reader cardinality limit. `producer.stateSize` exposes committed worker, series and retired-series counts. Call `producer.shutdown()` to cancel an in-flight round and release retained state.

There are no delta conversion, exponential histograms, exemplars, multiple resources, worker threads or PM2-specific integration in v0. Exemplars are not transported. Delta replies and exponential histograms fail explicitly. Worker readers always select cumulative temporality.

## Run the demo and checks

```sh
npm ci
npm test
npm run demo
```

The demo starts two real workers using SDK instruments, scrapes through the experimental exporter hook, checks the combined counter, separate gauges and explicit histogram buckets, then verifies that a repeated scrape has unchanged values. It exits after the checks.

`node examples/otlp-push.cjs` runs the OTLP version described in Push over OTLP instead.

`examples/grafana` runs a four-worker app with this package next to the same app on the stock exporter, with Prometheus and a Grafana dashboard that shows the difference. There's a live copy on Grafana Cloud linked from its README.

The release tests also cover all gauge reducers, worker exit and restart, missing workers, duplicate and late replies, invalid resources, schema conflicts, incompatible buckets and descriptors, cumulative resets, disappearing attributes, non-finite number transport, bounded state across worker churn, and HTTP failure/recovery. The persistence tests restart a real two-worker primary and check continuous counters, histogram buckets and start timestamps, hard crashes, final snapshots, incompatible files and options, write failures, competing writers and restored series limits. The lifecycle tests cover a worker that records and exits between scrapes, `forceFlush` and `shutdown` finals, the push interval bound on a hard kill, `beforeExit` on an unreferenced channel, disconnect, pushes and finals during a round, hundreds of short-lived workers with bounded state and memory, randomized worker lifecycles over 200 seeds, and the single-task example.

Sources for the pinned integration: [MetricReader producer hook](https://github.com/open-telemetry/opentelemetry-js/blob/12a5817ad147c186a0e45a6637abd5abf423727a/packages/sdk-metrics/src/export/MetricReader.ts), [Prometheus exporter](https://github.com/open-telemetry/opentelemetry-js/blob/12a5817ad147c186a0e45a6637abd5abf423727a/experimental/packages/opentelemetry-exporter-prometheus/src/PrometheusExporter.ts), [metrics data model](https://github.com/open-telemetry/opentelemetry-specification/blob/5740e9af3c82a3c89ef0b2357548946fbf5a9f5e/specification/metrics/data-model.md).
