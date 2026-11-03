// A Node cluster web app with four workers, run two ways.
// MODE=merged: workers report to the primary and @adityareddy-dev/otel-cluster-metrics
//   serves one /metrics for the whole cluster.
// MODE=stock: each worker runs the stock Prometheus exporter on the same port,
//   so the cluster hands every scrape to whichever worker accepts it.
// Every few minutes the primary retires a worker and forks a new one, the way
// a deploy or a memory limit would.
const cluster = require('node:cluster');
const { createServer } = require('node:http');
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { WorkerMetricReader, ClusterMetricProducer, createPrometheusHandler } = require('@adityareddy-dev/otel-cluster-metrics');

const MODE = process.env.MODE ?? 'merged';
const WORKERS = Number(process.env.WORKERS ?? 4);
const APP_PORT = Number(process.env.APP_PORT ?? 8080);
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9464);
const RECYCLE_MS = Number(process.env.RECYCLE_MS ?? 240000);
const resource = resourceFromAttributes({
  'service.name': MODE === 'merged' ? 'shop-api' : 'shop-api-stock',
  'service.namespace': 'golden-grot-demo',
});

if (cluster.isPrimary) {
  let server, provider, producer;
  if (MODE === 'merged') {
    producer = new ClusterMetricProducer({ resource, gaugeReducers: { 'queue.depth': 'sum' } });
    const exporter = new PrometheusExporter({ preventServerStart: true, metricProducers: [producer] });
    provider = new MeterProvider({ resource, readers: [exporter], sdkMetricsEnabled: false });
    const handler = createPrometheusHandler(exporter);
    server = createServer((req, res) => {
      if (req.url === '/metrics') void handler(req, res);
      else { res.writeHead(404); res.end(); }
    });
  }
  let ready = 0;
  const fork = () => {
    const w = cluster.fork();
    w.on('message', (m) => {
      if (m && m.ready && ++ready === WORKERS && server) server.listen(METRICS_PORT);
    });
    return w;
  };
  for (let i = 0; i < WORKERS; i++) fork();
  cluster.on('exit', (w, code, signal) => {
    if (!stopping) { console.log(`worker ${w.id} left (${signal ?? code}), forking a new one`); fork(); }
  });
  // Retire the oldest worker on a schedule. Most leave cleanly through SIGTERM;
  // one in five is killed outright, like an out-of-memory kill.
  setInterval(() => {
    const ids = Object.keys(cluster.workers);
    if (ids.length < WORKERS) return;
    const w = cluster.workers[ids[0]];
    w.process.kill(Math.random() < 0.2 ? 'SIGKILL' : 'SIGTERM');
  }, RECYCLE_MS).unref();
  let stopping = false;
  process.on('SIGTERM', async () => {
    stopping = true;
    for (const w of Object.values(cluster.workers)) w.process.kill('SIGTERM');
    if (server) server.close();
    if (provider) await provider.shutdown();
    if (producer) await producer.shutdown();
    process.exit(0);
  });
} else {
  const readers = [];
  if (MODE === 'merged') readers.push(new WorkerMetricReader());
  else readers.push(new PrometheusExporter({ host: '0.0.0.0', port: METRICS_PORT }));
  const provider = new MeterProvider({ resource, readers, sdkMetricsEnabled: false });
  const meter = provider.getMeter('shop-api', '1.0.0');
  const requests = meter.createCounter('shop.requests', { description: 'Requests handled.', unit: '{request}' });
  const duration = meter.createHistogram('shop.request.duration', {
    description: 'Time to handle a request.',
    unit: 's',
    advice: { explicitBucketBoundaries: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1] },
  });
  const active = meter.createUpDownCounter('shop.active_requests', { description: 'Requests in progress.', unit: '{request}' });
  // Each checkout queues a confirmation email; the worker sends one every 150 ms.
  let queued = 0;
  setInterval(() => { if (queued > 0) queued -= 1; }, 150).unref();
  meter.createObservableGauge('queue.depth', { description: 'Emails waiting to be sent by this worker.', unit: '{job}' })
    .addCallback((r) => r.observe(queued));
  meter.createObservableGauge('worker.heap.used', { description: 'V8 heap in use.', unit: 'By' })
    .addCallback((r) => r.observe(process.memoryUsage().heapUsed));

  const ROUTES = { '/products': [0.004, 0.02], '/cart': [0.006, 0.03], '/checkout': [0.03, 0.25] };
  createServer((req, res) => {
    const route = ROUTES[req.url] ? req.url : '/other';
    const [lo, hi] = ROUTES[route] ?? [0.001, 0.005];
    const start = performance.now();
    active.add(1, { 'http.route': route });
    const wait = (lo + Math.random() * Math.random() * (hi - lo) * 3) * 1000;
    setTimeout(() => {
      const status = route === '/checkout' && Math.random() < 0.03 ? 500 : 200;
      res.writeHead(status); res.end('ok');
      if (route === '/checkout' && status === 200) queued += 1 + Math.floor(Math.random() * 3);
      active.add(-1, { 'http.route': route });
      const attrs = { 'http.route': route, 'http.response.status_code': status };
      requests.add(1, attrs);
      duration.record((performance.now() - start) / 1000, attrs);
    }, wait);
  }).listen(APP_PORT);

  process.send({ ready: true });
  process.on('SIGTERM', async () => { await provider.shutdown(); process.exit(0); });
}
