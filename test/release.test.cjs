const {test} = require('node:test');
const assert = require('node:assert/strict');
const cluster = require('node:cluster');
const { once } = require('node:events');
const { createServer } = require('node:http');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const {ClusterMetricProducer,createPrometheusHandler} = require('../dist');
cluster.setupPrimary({exec:path.join(__dirname,'worker.cjs')});
const resource=resourceFromAttributes({'service.name':'test'});
let command=0;
async function set(w,state) { const id=++command; const done=new Promise(resolve=>{const cb=m=>{if(m.ack===id){w.off('message',cb);resolve();}};w.on('message',cb);});w.send({command:'set',id,state});await done; }
async function stop(w) { if(!w.isDead()) { const p=once(w,'exit');w.kill();await p; } }
async function setup(t,n=2,options={}) {
  const workers=Array.from({length:n},()=>cluster.fork());
  await Promise.all(workers.map(w=>new Promise(resolve=>w.on('message',m=>{if(m.ready)resolve();}))));
  const producer=new ClusterMetricProducer({resource,timeoutMillis:300,...options});
  t.after(async()=>{await producer.shutdown();await Promise.all(workers.map(stop));});
  return {workers,producer};
}
function metrics(result) { return Object.fromEntries(result.resourceMetrics.scopeMetrics.flatMap(s=>s.metrics.map(m=>[m.descriptor.name,m]))); }
async function values(p) { return metrics(await p.collect()); }

test('two real SDK workers through the experimental exporter hook',()=>{
  const result=execFileSync(process.execPath,['examples/two-workers.cjs'],{cwd:path.join(__dirname,'..'),timeout:15000,encoding:'utf8'});
  assert.match(result,/Repeated scrape unchanged/);
});
test('latest snapshots, explicit buckets, gauges and simultaneous scrapes',async t=>{
  const {workers,producer}=await setup(t);
  await set(workers[1],{counter:2,gauge:20,updown:3});
  const first=producer.collect();assert.strictEqual(producer.collect(),first);
  const m=metrics(await first);
  assert.equal(m.requests.dataPoints[0].value,3);assert.equal(m.active.dataPoints[0].value,5);
  assert.deepEqual(m.depth.dataPoints.map(p=>p.value).sort((a,b)=>a-b),[10,20]);
  assert(m.depth.dataPoints.every(p=>p.attributes['cluster.worker.id']));
  assert.deepEqual(m.latency.dataPoints[0].value.buckets,{boundaries:[10,20],counts:[2,0,0]});
  assert.equal(m.latency.dataPoints[0].value.count,2);
  assert.equal(m.latency.dataPoints[0].value.sum,10);
  const start=m.requests.dataPoints[0].startTime;
  await set(workers[0],{counter:4});
  const next=await values(producer);assert.equal(next.requests.dataPoints[0].value,6);
  assert.deepEqual(next.requests.dataPoints[0].startTime,start);
});
test('counters and histograms carry no cluster.worker.id attribute by default',async t=>{
  const {workers,producer}=await setup(t);
  await set(workers[1],{counter:2,updown:3});
  const live=await values(producer);
  for(const name of ['requests','active','latency']) {
    assert.equal(live[name].dataPoints.length,1,name);
    assert(live[name].dataPoints.every(p=>!('cluster.worker.id' in p.attributes)),name);
  }
  assert.equal(live.depth.dataPoints.length,2);assert(live.depth.dataPoints.every(p=>p.attributes['cluster.worker.id']));
  await stop(workers[0]);
  const retired=await values(producer);assert.equal(retired.requests.dataPoints[0].value,3);
  for(const name of ['requests','latency']) assert(retired[name].dataPoints.every(p=>!('cluster.worker.id' in p.attributes)),name);
});
for(const [reducer,expected] of [['sum',30],['min',10],['max',20],['mean',15]]) test('gauge reducer '+reducer,async t=>{
  const {workers,producer}=await setup(t,2,{gaugeReducers:{depth:reducer}});
  await set(workers[1],{gauge:20});const m=await values(producer);
  assert.equal(m.depth.dataPoints.length,1);assert.equal(m.depth.dataPoints[0].value,expected);
});
test('worker exit retains monotonic and histogram totals, removes gauges and nonmonotonic sums; restart adds once',async t=>{
  const {workers,producer}=await setup(t);
  await values(producer);await stop(workers[0]);
  let m=await values(producer);assert.equal(m.requests.dataPoints[0].value,2);assert.equal(m.depth.dataPoints.length,1);assert.equal(m.active.dataPoints[0].value,2);assert.equal(m.latency.dataPoints[0].value.count,2);
  const replacement=cluster.fork();workers.push(replacement);await once(replacement,'message');
  m=await values(producer);assert.equal(m.requests.dataPoints[0].value,3);assert.equal(m.depth.dataPoints.length,2);
  assert.equal((await values(producer)).requests.dataPoints[0].value,3);
});
test('missing worker fails round without changing committed state; returns without double count',async t=>{
  const {workers,producer}=await setup(t);await values(producer);const before=producer.stateSize;
  await set(workers[0],{missing:true});await assert.rejects(producer.collect(),/Missing workers/);assert.deepEqual(producer.stateSize,before);
  await set(workers[0],{missing:false,counter:3});assert.equal((await values(producer)).requests.dataPoints[0].value,4);
});
test('duplicate and late replies are discarded, listeners do not accumulate',async t=>{
  const diagnostics=[];const {workers,producer}=await setup(t,2,{timeoutMillis:100,onDiagnostic:m=>diagnostics.push(m)});
  await set(workers[0],{duplicate:true});await set(workers[1],{delay:30});
  assert.equal((await values(producer)).requests.dataPoints[0].value,2);assert(diagnostics.some(m=>m.includes('duplicate')));
  await set(workers[0],{delay:150,duplicate:false});await assert.rejects(producer.collect(),/Missing workers/);
  await set(workers[0],{delay:80});assert.equal((await values(producer)).requests.dataPoints[0].value,2);
  assert(diagnostics.some(m=>m.includes('late')));
  for(const w of workers) assert.equal(w.listenerCount('exit'),0);
});
for(const [state,error] of [
  [{resource:{'service.name':'other'}},/Resource conflict/],
  [{schemaUrl:'https://example.org/schema'},/Resource conflict/],
  [{boundaries:[11,20]},/Mismatched bucket boundaries/],
  [{temporality:0},/cumulative/],
  [{version:2},/Unsupported IPC/],
  [{wrongBoot:true},/Invalid reply/],
  [{error:true},/fixture failure/],
  [{counts:[0,0,0]},/Invalid histogram buckets/]
]) test('invalid reply fails atomically '+JSON.stringify(state),async t=>{
  const {workers,producer}=await setup(t);await values(producer);const before=producer.stateSize;
  await set(workers[0],state);await assert.rejects(producer.collect(),error);assert.deepEqual(producer.stateSize,before);
});
test('explicit resource exclusions and primary resource preservation',async t=>{
  const {workers,producer}=await setup(t,2,{omitResourceAttributes:['process.pid']});
  for(const [i,w] of workers.entries()) await set(w,{resource:{'service.name':'test','process.pid':i}});
  assert.deepEqual((await producer.collect()).resourceMetrics.resource.attributes,resource.attributes);
});
test('point reset, disappearing attributes, reappearance and stale sequence',async t=>{
  const {workers,producer}=await setup(t,1);await values(producer);
  await set(workers[0],{empty:true});assert.equal((await values(producer)).requests.dataPoints[0].value,1);
  await set(workers[0],{empty:false,counter:2});assert.equal((await values(producer)).requests.dataPoints[0].value,2);
  await set(workers[0],{counter:1,start:[2,0]});assert.equal((await values(producer)).requests.dataPoints[0].value,3);
  assert.equal((await values(producer)).latency.dataPoints[0].value.count,2);
  await set(workers[0],{stale:true});await assert.rejects(producer.collect(),/Stale worker sequence/);
});
test('bounded series, bytes and workers',async t=>{
  const {workers,producer}=await setup(t,1,{maxSeries:8,maxMessageBytes:3000,maxWorkers:1});
  for(let i=0;i<3;i++) { await set(workers[0],{attrs:{id:i}});await values(producer); }
  assert.equal(producer.stateSize.series,8);
  await set(workers[0],{attrs:{id:4}});await assert.rejects(producer.collect(),/series limit/);assert.equal(producer.stateSize.series,8);
  await set(workers[0],{oversize:true});await assert.rejects(producer.collect(),/byte limit/);
  const extra=cluster.fork();workers.push(extra);await once(extra,'message');await assert.rejects(producer.collect(),/Worker limit/);
});
test('HTTP 503 on incomplete scrape, 200 on recovery through exporter hook',async t=>{
  const {workers,producer}=await setup(t,1);
  const exporter=new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
  const provider=new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});
  const server=createServer(createPrometheusHandler(exporter));server.listen(0,'127.0.0.1');await once(server,'listening');
  t.after(async()=>{await new Promise(resolve=>server.close(resolve));await provider.shutdown();});
  const url=`http://127.0.0.1:${server.address().port}`;
  await set(workers[0],{missing:true});let r=await fetch(url);assert.equal(r.status,503);assert.doesNotMatch(await r.text(),/requests_total/);
  await set(workers[0],{missing:false});r=await fetch(url);assert.equal(r.status,200);assert.match(await r.text(),/requests_total/);
});
test('retired epochs fold into fixed totals across repeated process restarts',async t=>{
  const {workers,producer}=await setup(t,1,{maxSeries:6});
  for(let i=0;i<12;i++) {
    assert.equal((await values(producer)).requests.dataPoints[0].value,i+1);
    assert(producer.stateSize.series <= 6);
    await stop(workers.at(-1));
    if(i<11) {const w=cluster.fork();workers.push(w);await once(w,'message');}
  }
  const m=await values(producer);assert.equal(m.requests.dataPoints[0].value,12);assert.equal(m.latency.dataPoints[0].value.count,12);assert.equal(m.depth,undefined);assert.equal(m.active,undefined);assert.equal(producer.stateSize.retiredSeries,2);assert.equal(producer.stateSize.workers,0);
});
test('attribute order is normalized and attribute value types remain distinct',async t=>{
  const {workers,producer}=await setup(t);
  await set(workers[0],{attrs:{a:1,b:'x'}});await set(workers[1],{attrs:{b:'x',a:1}});
  assert.equal((await values(producer)).requests.dataPoints.length,1);
  await set(workers[1],{attrs:{a:'1',b:'x'}});
  assert.equal((await values(producer)).requests.dataPoints.length,2);
});
test('nonfinite gauges survive JSON without becoming null',async t=>{
  const {workers,producer}=await setup(t,1);
  // The fixture command itself uses JSON IPC. Request tagged value via additional metrics.
  await set(workers[0],{extra:[{descriptor:{name:'temperature',description:'temperature',unit:'',valueType:1},aggregationTemporality:1,dataPointType:2,dataPoints:[{attributes:{a:'x'},startTime:[1,0],endTime:[2,0],value:42}]}]});
  const {encodeMetrics,decodeMetrics,attributeKey}=require('../dist/protocol');
  for(const n of [NaN,Infinity,-Infinity]) {
    const input={resource,scopeMetrics:[{scope:{name:'numbers'},metrics:[{descriptor:{name:'gauge',description:'gauge',unit:'',valueType:1},aggregationTemporality:1,dataPointType:2,dataPoints:[{attributes:{n},startTime:[1,0],endTime:[2,0],value:n}]}]}]};
    const out=decodeMetrics(JSON.parse(JSON.stringify(encodeMetrics(input))),10);
    assert(Object.is(out.scopeMetrics[0].metrics[0].dataPoints[0].value,n));
    assert(Object.is(out.scopeMetrics[0].metrics[0].dataPoints[0].attributes.n,n));
  }
  assert.notEqual(attributeKey({a:1}),attributeKey({a:'1'}));
  assert.equal((await values(producer)).temperature.dataPoints[0].value,42);
});
test('unsupported exponential histograms, descriptor conflicts and future timestamps fail',async t=>{
  const {workers,producer}=await setup(t);
  const base={descriptor:{name:'requests',description:'other',unit:'seconds',valueType:1},aggregationTemporality:1,dataPointType:3,isMonotonic:true,dataPoints:[{attributes:{},startTime:[1,0],endTime:[2,0],value:1}]};
  // A distinct scope is not used here: duplicate name in one worker must also fail.
  await set(workers[0],{extra:[base]});await assert.rejects(producer.collect(),/Duplicate metric/);
  await set(workers[0],{extra:[{...base,descriptor:{...base.descriptor,name:'exp'},dataPointType:1}]});await assert.rejects(producer.collect(),/Unsupported data point/);
  await set(workers[0],{extra:[],endTime:[Math.floor(Date.now()/1000)+100,0]});await assert.rejects(producer.collect(),/after collection/);
});
test('monotonic decrease without reset fails and shutdown cancels a pending round',async t=>{
  const {workers,producer}=await setup(t,1);await set(workers[0],{counter:5});await values(producer);
  await set(workers[0],{counter:2});await assert.rejects(producer.collect(),/decreased without a reset/);
  await set(workers[0],{missing:true});const pending=producer.collect();await producer.shutdown();await assert.rejects(pending,/shutdown/);await assert.rejects(producer.collect(),/shutdown/);assert.equal(producer.stateSize.series,0);
});
test('histogram extrema only survive when all nonempty populations provide them',()=>{
  const {add}=require('../dist/merge');
  const row=(count,min,max)=>({scope:{name:'x'},metric:{dataPointType:0,aggregationTemporality:1,descriptor:{name:'h',unit:'',description:'',valueType:1}},point:{attributes:{},value:{count,sum:count,buckets:{boundaries:[1],counts:[count,0]},min,max}}});
  assert.equal(add(row(1,1,3),row(1,0,5)).point.value.min,0);
  assert.equal(add(row(1,1,3),row(1,0,5)).point.value.max,5);
  assert.equal(add(row(1,1,3),row(1,undefined,5)).point.value.min,undefined);
  assert.equal(add(row(0,undefined,undefined),row(1,1,3)).point.value.min,1);
  assert.throws(()=>add(row(1,1,3),{...row(1,1,3),metric:{...row(1,1,3).metric,descriptor:{name:'h',unit:'seconds',description:'',valueType:1}}}),/Incompatible descriptor/);
});
test('shutdown before an empty collection resolves does not restore state',async()=>{
  const p=new ClusterMetricProducer({resource});const result=p.collect();await p.shutdown();await assert.rejects(result,/shutdown/);assert.equal(p.stateSize.series,0);
});
