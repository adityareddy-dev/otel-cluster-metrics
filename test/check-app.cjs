// A small cluster app for the check tests. MODE=stock runs the stock Prometheus exporter
// in every worker on one shared port. MODE=merged serves one /metrics from the primary.
const cluster = require('node:cluster');
const { createServer } = require('node:http');
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { WorkerMetricReader, ClusterMetricProducer, createPrometheusHandler } = require('../dist');
const MODE = process.env.MODE, WORKERS = Number(process.env.WORKERS ?? 4);
const APP_PORT = Number(process.env.APP_PORT), METRICS_PORT = Number(process.env.METRICS_PORT);
const resource = resourceFromAttributes({'service.name':'check-app'});

if (cluster.isPrimary) {
  let server, ready = 0;
  if (MODE === 'merged') {
    const producer = new ClusterMetricProducer({resource});
    const exporter = new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
    new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});
    server = createServer(createPrometheusHandler(exporter));
  }
  for (let i = 0; i < WORKERS; i++) cluster.fork().on('message', m => {
    if (m && m.ready && ++ready === WORKERS) {
      if (server) server.listen(METRICS_PORT, '127.0.0.1', () => process.send?.('listening'));
      else process.send?.('listening');
    }
  });
  process.on('SIGTERM', async () => {
    await Promise.all(Object.values(cluster.workers).map(w => new Promise(resolve => {
      if (w.isDead()) return resolve();
      w.once('exit', resolve); w.kill('SIGTERM');
    })));
    server?.close(); process.exit(0);
  });
} else {
  const reader = MODE === 'merged' ? new WorkerMetricReader() : new PrometheusExporter({host:'127.0.0.1',port:METRICS_PORT});
  const provider = new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false});
  const requests = provider.getMeter('check-app').createCounter('requests');
  // Wait for the stock metrics listener too, before announcing that the app is ready.
  Promise.resolve(MODE === 'stock' ? reader.startServer() : undefined).then(() => {
    createServer((req, res) => { requests.add(1); res.end('ok'); }).listen(APP_PORT, '127.0.0.1', () => process.send({ready:true}));
  });
  process.on('SIGTERM', () => process.exit(0));
}
