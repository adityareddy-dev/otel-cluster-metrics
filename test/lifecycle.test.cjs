const {test} = require('node:test');
const assert = require('node:assert/strict');
const cluster = require('node:cluster');
const { once } = require('node:events');
const { execFileSync } = require('node:child_process');
const path = require('node:path');
const v8 = require('node:v8');
const vm = require('node:vm');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const {ClusterMetricProducer} = require('../dist');
const resource=resourceFromAttributes({'service.name':'test'});
let command=0;
function fork(file,env={}) { cluster.setupPrimary({exec:path.join(__dirname,file)}); return cluster.fork(env); }
function ready(w) { return new Promise(resolve=>{const cb=m=>{if(m&&m.ready){w.off('message',cb);resolve(w);}};w.on('message',cb);}); }
async function send(w,message) { const id=++command; const done=new Promise(resolve=>{const cb=m=>{if(m&&m.ack===id){w.off('message',cb);resolve();}};w.on('message',cb);}); w.send({...message,id}); await done; }
async function stop(w,signal='SIGTERM') { if(!w.isDead()) { const exited=once(w,'exit'); w.process.kill(signal); await exited; } }
function producer(t,options={}) { const p=new ClusterMetricProducer({resource,timeoutMillis:1000,...options}); t.after(()=>p.shutdown()); return p; }
function tracked(t) { const workers=[]; t.after(async()=>{ for(const w of workers) await stop(w,'SIGKILL'); }); return (file,env)=>{ const w=fork(file,env); workers.push(w); return w; }; }
async function total(p,name='requests') { const r=await p.collect(); const m=r.resourceMetrics.scopeMetrics.flatMap(s=>s.metrics).find(m=>m.descriptor.name===name); return m ? m.dataPoints.reduce((n,x)=>n+(typeof x.value==='number' ? x.value : x.value.count),0) : 0; }
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

test('a worker that starts, records and exits between scrapes keeps its increments',async t=>{
  const p=producer(t), fork=tracked(t);
  const w=fork('sdk-worker.cjs',{MODE:'once',COUNT:'5'}); await once(w,'exit');
  assert.equal(await total(p),5); assert.equal(await total(p,'latency'),1);
  assert.deepEqual(p.stateSize,{workers:0,series:2,retiredSeries:2});
});
test('forceFlush pushes now and provider.shutdown() sends the final snapshot before exit',async t=>{
  const p=producer(t), fork=tracked(t);
  const a=await ready(fork('sdk-worker.cjs')); assert.equal(await total(p),1);
  await send(a,{command:'add',value:4}); await send(a,{command:'flush'}); await stop(a,'SIGKILL');
  assert.equal(await total(p),5);
  const b=await ready(fork('sdk-worker.cjs')); await send(b,{command:'add',value:2});
  const exited=once(b,'exit'); b.send({command:'exit',id:++command}); await exited;
  assert.equal(await total(p),8); assert.equal(p.stateSize.workers,0);
});
test('a periodic push bounds what a hard kill loses to the push interval',async t=>{
  const p=producer(t), fork=tracked(t);
  const pushing=await ready(fork('sdk-worker.cjs',{PUSH:'40'})); await send(pushing,{command:'add',value:3}); await sleep(200); await stop(pushing,'SIGKILL');
  assert.equal(await total(p),4);
  const silent=await ready(fork('sdk-worker.cjs')); assert.equal(await total(p),5); await send(silent,{command:'add',value:3}); await stop(silent,'SIGKILL');
  assert.equal(await total(p),5); // The 3 recorded after the last report are lost, as documented.
});
test('without pushIntervalMillis the reader pushes on the shipped 10000 ms default',async t=>{
  const {DEFAULT_PUSH_INTERVAL_MILLIS}=require('../dist'); assert.equal(DEFAULT_PUSH_INTERVAL_MILLIS,10000);
  const fork=tracked(t), forkedAt=Date.now(), w=fork('sdk-worker.cjs',{PUSH:'default'});
  const pushed=new Promise(resolve=>w.on('message',m=>{ if(typeof m==='string' && JSON.parse(m).kind==='push') resolve(Date.now()); }));
  await ready(w); const readyAt=Date.now(), pushedAt=await pushed;
  // The timer starts in the reader constructor, after the fork and before the ready message.
  assert(pushedAt-forkedAt >= 10000, `first push ${pushedAt-forkedAt} ms after fork`);
  assert(pushedAt-readyAt <= 12000, `first push ${pushedAt-readyAt} ms after ready`);
});
test('beforeExit sends the final snapshot when the application unreferences the channel',async t=>{
  const p=producer(t), fork=tracked(t);
  const w=fork('sdk-worker.cjs',{MODE:'unref',COUNT:'3'}); const [code]=await once(w,'exit');
  assert.equal(code,0); assert.equal(await total(p),3);
});
test('a disconnected worker is retired on the event and later rounds skip it',async t=>{
  const p=producer(t), fork=tracked(t);
  const w=await ready(fork('worker.cjs')); assert.equal(await total(p),1);
  const exited=once(w,'exit'); w.disconnect(); await once(w,'disconnect');
  assert.equal(await total(p),1); assert.deepEqual(p.stateSize,{workers:0,series:2,retiredSeries:2});
  await exited;
});
test('a push after the reply is applied, and an exit after the reply does not fail the round',async t=>{
  const p=producer(t), fork=tracked(t);
  const w=await ready(fork('worker.cjs')); await send(w,{command:'set',state:{pushThenExit:9}});
  const exited=once(w,'exit'); assert.equal(await total(p),1); await exited;
  assert.equal(await total(p),9); assert.equal(p.stateSize.workers,0);
});
test('a push arriving during a round is held until the round commits',async t=>{
  const diagnostics=[]; const p=producer(t,{onDiagnostic:m=>diagnostics.push(m)}), fork=tracked(t);
  const w=await ready(fork('worker.cjs')); await send(w,{command:'set',state:{delay:100}});
  const round=total(p); await sleep(20); await send(w,{command:'emit',kind:'push',counter:5});
  assert.equal(await round,5); assert.deepEqual(diagnostics,[]); assert.equal(await total(p),5);
});
test('a final snapshot during a round counts as that worker\'s reply',async t=>{
  const p=producer(t), fork=tracked(t);
  const w=await ready(fork('worker.cjs')); await send(w,{command:'set',state:{finalThenExit:7}});
  const exited=once(w,'exit'); assert.equal(await total(p),7); await exited;
  assert.equal(await total(p),7); assert.deepEqual(p.stateSize,{workers:0,series:2,retiredSeries:2});
});
test('an exit before the reply fails the round, and a backward push is discarded with a diagnostic',async t=>{
  const diagnostics=[]; const p=producer(t,{onDiagnostic:m=>diagnostics.push(m)}), fork=tracked(t);
  const w=await ready(fork('worker.cjs')); await send(w,{command:'set',state:{counter:4}}); assert.equal(await total(p),4);
  await send(w,{command:'emit',kind:'push',counter:2});
  assert(diagnostics.some(m=>/push from worker .* decreased without a reset/.test(m)),diagnostics.join('\n'));
  await send(w,{command:'set',state:{counter:4}}); assert.equal(await total(p),4);
  await send(w,{command:'set',state:{exitOnCollect:true}}); await assert.rejects(p.collect(),/(exited|disconnected) during collection/);
  assert.equal(await total(p),4);
});
test('hundreds of short-lived workers keep state and memory bounded and totals never go backward',async t=>{
  const p=producer(t,{maxSeries:70,timeoutMillis:5000}); // 20 overlapping workers hold 60 live series plus 2 retired totals; one leaked worker would breach this
  t.after(async()=>{ for(const w of Object.values(cluster.workers)) if(w) await stop(w,'SIGKILL'); });
  v8.setFlagsFromString('--expose-gc'); const gc=vm.runInNewContext('gc');
  const heap=()=>{ gc(); gc(); return process.memoryUsage().heapUsed; };
  let finals=0, last=0, base=0;
  const check=async()=>{
    const n=await total(p); assert(n>=last,`total went backward: ${last} to ${n}`); assert.equal(n,finals); last=n;
    assert(p.stateSize.series<=2+3*Object.keys(cluster.workers).length); assert(p.stateSize.retiredSeries<=2);
  };
  for(let i=1;i<=200;i++) {
    const final=i%5!==0; const w=fork('churn-worker.cjs',{FINAL:final?'1':'0'}); await once(w,'exit'); if(final) finals++;
    if(i%25===0) await check();
    if(i===50) base=heap();
  }
  for(let batch=0;batch<5;batch++) {
    const workers=Array.from({length:20},()=>fork('churn-worker.cjs',{FINAL:'1'})); finals+=20;
    const scrape=p.collect().then(()=>'ok',e=>e.message); // overlaps the batch
    await Promise.all(workers.map(w=>once(w,'exit')));
    assert.match(await scrape,/^ok$|during collection/);
    await check();
  }
  assert.equal(last,260);
  const grown=heap()-base; assert(grown<8*1024*1024,`heap grew ${grown} bytes over 250 workers`);
  assert.equal(cluster.listenerCount('message'),1); assert.equal(Object.keys(cluster.workers).length,0);
});
test('single-task workers report in band and their counts survive their exit',()=>{
  const result=execFileSync(process.execPath,['examples/single-task-workers.cjs'],{cwd:path.join(__dirname,'..'),timeout:15000,encoding:'utf8'});
  assert.match(result,/In-band results: 12\. Scraped total after every worker exited: 12\./);
});
