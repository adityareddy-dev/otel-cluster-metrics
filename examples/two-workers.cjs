const cluster = require('node:cluster');
const { createServer } = require('node:http');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { MeterProvider, AggregationType } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { WorkerMetricReader, ClusterMetricProducer, createPrometheusHandler } = require('../dist');
const resource = resourceFromAttributes({'service.name':'cluster-demo'});

async function main() {
  if (cluster.isWorker) {
    const reader = new WorkerMetricReader();
    const provider = new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false,views:[{instrumentName:'latency',aggregation:{type:AggregationType.EXPLICIT_BUCKET_HISTOGRAM,options:{boundaries:[10,20]}}}]});
    const meter = provider.getMeter('demo','1.0');
    const n = Number(process.env.WORKER_NUMBER);
    meter.createCounter('requests').add(n);
    meter.createGauge('queue_depth').record(n*10);
    const histogram = meter.createHistogram('latency');
    histogram.record(5); histogram.record(15);
    process.send({ready:true});
    return;
  }
  const workers = [1,2].map(n=>cluster.fork({WORKER_NUMBER:String(n)}));
  let server, provider, producer;
  try {
    await Promise.all(workers.map(w=>new Promise(resolve=>w.on('message',m=>{if(m.ready) resolve();}))));
    producer = new ClusterMetricProducer({resource});
    const exporter = new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
    provider = new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});
    server = createServer(createPrometheusHandler(exporter));
    server.listen(0,'127.0.0.1'); await once(server,'listening');
    const url = `http://127.0.0.1:${server.address().port}/metrics`;
    const response = await fetch(url), body = await response.text();
    assert.equal(response.status,200);
    assert.match(body,/requests_total[^\n]* 3(?:\n|$)/);
    assert.match(body,/queue_depth\{[^\n]*cluster_worker_id=[^\n]* 10(?:\n|$)/);
    assert.match(body,/queue_depth\{[^\n]*cluster_worker_id=[^\n]* 20(?:\n|$)/);
    assert.match(body,/latency_count[^\n]* 4(?:\n|$)/);
    assert.match(body,/latency_sum[^\n]* 40(?:\n|$)/);
    assert.match(body,/latency_bucket\{[^\n]*le="10"[^\n]* 2(?:\n|$)/);
    assert.match(body,/latency_bucket\{[^\n]*le="20"[^\n]* 4(?:\n|$)/);
    const again = await fetch(url); assert.equal(await again.text(),body);
    console.log(body);
    console.log('Two workers, correct counters, separate gauges and explicit buckets. Repeated scrape unchanged.');
  } finally {
    if (server) await new Promise(resolve=>server.close(resolve));
    if (provider) await provider.shutdown();
    if (producer) await producer.shutdown();
    await Promise.all(workers.map(async w=>{if (!w.isDead()) { const exited=once(w,'exit');w.kill();await exited; }}));
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
