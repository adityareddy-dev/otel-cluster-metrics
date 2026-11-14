// Push the merged cluster totals over OTLP/HTTP, with no Prometheus scrape in between.
// Without OTEL_EXPORTER_OTLP_METRICS_ENDPOINT it starts a local receiver and checks what arrives.
// With it set (and OTEL_EXPORTER_OTLP_HEADERS for auth, for example Grafana Cloud's OTLP gateway),
// it sends the same export there instead.
const cluster = require('node:cluster');
const { createServer } = require('node:http');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const { MeterProvider, PeriodicExportingMetricReader } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { OTLPMetricExporter } = require('@opentelemetry/exporter-metrics-otlp-http');
const { WorkerMetricReader, ClusterMetricProducer } = require('../dist');
const resource = resourceFromAttributes({'service.name':'cluster-otlp-demo'});

async function localReceiver() {
  const bodies = [];
  const server = createServer((req,res)=>{
    let raw=''; req.setEncoding('utf8'); req.on('data',c=>raw+=c);
    req.on('end',()=>{ if (req.url==='/v1/metrics') bodies.push(JSON.parse(raw)); res.writeHead(200,{'content-type':'application/json'}); res.end('{}'); });
  });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  return {bodies,server,url:`http://127.0.0.1:${server.address().port}/v1/metrics`};
}

async function main() {
  if (cluster.isWorker) {
    const provider = new MeterProvider({resource,readers:[new WorkerMetricReader()],sdkMetricsEnabled:false});
    provider.getMeter('demo','1.0').createCounter('requests').add(Number(process.env.WORKER_NUMBER));
    process.send({ready:true});
    return;
  }
  const workers = [1,2].map(n=>cluster.fork({WORKER_NUMBER:String(n)}));
  const remote = process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT;
  let local, provider, producer;
  try {
    await Promise.all(workers.map(w=>new Promise(resolve=>w.on('message',m=>{if(m.ready) resolve();}))));
    if (!remote) local = await localReceiver();
    producer = new ClusterMetricProducer({resource});
    // The exporter reads OTEL_EXPORTER_OTLP_METRICS_ENDPOINT and OTEL_EXPORTER_OTLP_HEADERS when no url is given.
    const exporter = new OTLPMetricExporter(local ? {url:local.url} : {});
    // The periodic reader only logs a failed export, so keep the result to report it here.
    let result;
    const send = exporter.export.bind(exporter);
    exporter.export = (metrics,done)=>send(metrics,r=>{result=r;done(r);});
    const reader = new PeriodicExportingMetricReader({exporter,exportIntervalMillis:60000,metricProducers:[producer]});
    provider = new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false});
    await provider.forceFlush();
    if (!result || result.code !== 0) throw new Error(`Export failed: ${result?.error?.message ?? 'nothing was exported'}`);
    if (!local) { console.log(`Sent the merged totals to ${remote}.`); return; }
    const metric = local.bodies[0].resourceMetrics.flatMap(r=>r.scopeMetrics).flatMap(s=>s.metrics).find(m=>m.name==='requests');
    const point = metric.sum.dataPoints[0];
    assert.equal(Number(point.asInt ?? point.asDouble),3);
    console.log(JSON.stringify(metric,null,2));
    console.log('Two workers, one OTLP export, requests = 3.');
  } finally {
    if (provider) await provider.shutdown();
    if (producer) await producer.shutdown();
    if (local) await new Promise(resolve=>local.server.close(resolve));
    await Promise.all(workers.map(async w=>{if (!w.isDead()) { const exited=once(w,'exit');w.kill();await exited; }}));
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
