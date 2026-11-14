const {test} = require('node:test');
const assert = require('node:assert/strict');
const cluster = require('node:cluster');
const { once } = require('node:events');
const { createServer } = require('node:http');
const path = require('node:path');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { MeterProvider, PeriodicExportingMetricReader } = require('@opentelemetry/sdk-metrics');
const { OTLPMetricExporter } = require('@opentelemetry/exporter-metrics-otlp-http');
const { ClusterMetricProducer } = require('../dist');
cluster.setupPrimary({exec:path.join(__dirname,'worker.cjs')});
const resource=resourceFromAttributes({'service.name':'test'});
let command=0;
async function set(w,state) { const id=++command; const done=new Promise(resolve=>{const cb=m=>{if(m.ack===id){w.off('message',cb);resolve();}};w.on('message',cb);});w.send({command:'set',id,state});await done; }
async function stop(w) { if(!w.isDead()) { const p=once(w,'exit');w.kill();await p; } }

/** A local OTLP/HTTP receiver that keeps every metrics request body. */
async function receiver(t) {
  const bodies=[];
  const server=createServer((req,res)=>{
    let raw='';req.setEncoding('utf8');req.on('data',c=>raw+=c);
    req.on('end',()=>{ if(req.url==='/v1/metrics') bodies.push(JSON.parse(raw)); res.writeHead(200,{'content-type':'application/json'});res.end('{}'); });
  });
  server.listen(0,'127.0.0.1');await once(server,'listening');
  t.after(()=>new Promise(resolve=>server.close(resolve)));
  return {bodies,url:`http://127.0.0.1:${server.address().port}/v1/metrics`};
}
function exported(body,name) {
  for (const rm of body.resourceMetrics) for (const sm of rm.scopeMetrics) for (const m of sm.metrics) if (m.name===name) return m;
}
const value=p=>p.asInt !== undefined ? Number(p.asInt) : p.asDouble;

async function setup(t) {
  const workers=Array.from({length:2},()=>cluster.fork());
  await Promise.all(workers.map(w=>new Promise(resolve=>w.on('message',m=>{if(m.ready)resolve();}))));
  const {bodies,url}=await receiver(t);
  const producer=new ClusterMetricProducer({resource,timeoutMillis:300});
  const reader=new PeriodicExportingMetricReader({exporter:new OTLPMetricExporter({url}),exportIntervalMillis:600000,metricProducers:[producer]});
  const provider=new MeterProvider({resource,readers:[reader]});
  t.after(async()=>{await provider.shutdown().catch(()=>{});await producer.shutdown();await Promise.all(workers.map(stop));});
  return {workers,producer,provider,bodies};
}

test('the merged cluster totals go out over OTLP with no Prometheus in between',async t=>{
  const {workers,provider,bodies}=await setup(t);
  await set(workers[1],{counter:2});
  await provider.forceFlush();
  assert.equal(bodies.length,1);
  const requests=exported(bodies[0],'requests');
  assert.equal(requests.sum.dataPoints.length,1);
  assert.equal(value(requests.sum.dataPoints[0]),3);
  assert.equal(requests.sum.isMonotonic,true);
  assert.equal(requests.sum.aggregationTemporality,2); // cumulative
  const depth=exported(bodies[0],'depth');
  assert.equal(depth.gauge.dataPoints.length,2);
  const latency=exported(bodies[0],'latency');
  assert.equal(Number(latency.histogram.dataPoints[0].count),2);
});

test('a worker that exits keeps its count in the next OTLP export',async t=>{
  const {workers,provider,bodies}=await setup(t);
  await set(workers[1],{counter:2});
  await provider.forceFlush();
  await stop(workers[0]);
  await provider.forceFlush();
  assert.equal(bodies.length,2);
  assert.equal(value(exported(bodies[1],'requests').sum.dataPoints[0]),3);
  assert.equal(exported(bodies[1],'depth').gauge.dataPoints.length,1);
});

test('an incomplete round sends nothing, so the backend never sees a partial total',async t=>{
  const {workers,provider,bodies}=await setup(t);
  await provider.forceFlush();
  assert.equal(bodies.length,1);
  await set(workers[1],{missing:true});
  await provider.forceFlush().catch(()=>{});
  assert.equal(bodies.length,1);
  await set(workers[1],{missing:false,counter:5});
  await provider.forceFlush();
  assert.equal(bodies.length,2);
  assert.equal(value(exported(bodies[1],'requests').sum.dataPoints[0]),6);
});
