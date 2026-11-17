const {test} = require('node:test');
const assert = require('node:assert/strict');
const { fork } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
function folder(t) { const dir=fs.mkdtempSync(path.join(os.tmpdir(),'ocm-state-')); t.after(()=>fs.rmSync(dir,{recursive:true,force:true})); return path.join(dir,'totals.json'); }
let id=0;
async function primary(t,file) {
  const child=fork(path.join(__dirname,'restart-primary.cjs'),[file],{stdio:['ignore','ignore','inherit','ipc']});
  t.after(async()=>{if(child.exitCode===null && child.signalCode===null){const exited=once(child,'exit');child.kill('SIGKILL');await exited;}});
  await once(child,'message'); return child;
}
function command(child,message) {
  const n=++id;
  return new Promise((resolve,reject)=>{
    const f=m=>{if(m.ack!==n)return;child.off('message',f);m.error?reject(new Error(m.error)):resolve(m.result);};
    child.on('message',f);child.send({...message,id:n});
  });
}
const metric=(r,name)=>r.metrics.find(m=>m.descriptor.name===name);
test('two-worker primary restart preserves counters, histograms and the aggregate start', {timeout:15000}, async t=>{
  const file=folder(t), a=await primary(t,file);
  await command(a,{command:'record',count:3});
  const first=await command(a,{command:'scrape'});
  assert.equal(first.status,200);assert.match(first.body,/requests_total\{[^}]*\} 6/);
  assert.equal(metric(first,'latency').dataPoints[0].value.count,6);
  const exited=once(a,'exit');await command(a,{command:'stop'});await exited;
  const b=await primary(t,file);await command(b,{command:'record',count:2});
  const second=await command(b,{command:'scrape'});
  assert.equal(second.status,200);assert.match(second.body,/requests_total\{[^}]*\} 10/);
  assert.match(second.body,/latency_count\{[^}]*\} 10/);
  assert.equal(metric(second,'latency').dataPoints[0].value.sum,30);
  assert.deepEqual(metric(second,'latency').dataPoints[0].value.buckets,{boundaries:[10,20],counts:[10,0,0]});
  for(const name of ['requests','latency']) assert.deepEqual(metric(second,name).dataPoints[0].startTime,metric(first,name).dataPoints[0].startTime);
  assert.equal(metric(second,'active').dataPoints[0].value,2);
  assert.equal(metric(second,'depth').dataPoints.length,2);
  const done=once(b,'exit');await command(b,{command:'stop'});await done;
});

const { resourceFromAttributes }=require('@opentelemetry/resources');
const { ClusterMetricProducer }=require('../dist');
const resource=resourceFromAttributes({'service.name':'restart-test'});
function producer(t,file,options={}) {
  const p=new ClusterMetricProducer({resource,stateFile:file,...options});
  t.after(()=>p.shutdown());return p;
}
async function seed(t,file) {
  const p=await primary(t,file);await command(p,{command:'record',count:2});await command(p,{command:'scrape'});
  const exited=once(p,'exit');await command(p,{command:'stop'});await exited;
  return fs.readFileSync(file,'utf8');
}
test('missing state starts fresh and shutdown saves an empty aggregate start',async t=>{
  const file=folder(t),p=producer(t,file);await p.collect();await p.shutdown();
  const saved=JSON.parse(fs.readFileSync(file,'utf8'));
  assert.equal(saved.version,1);assert.deepEqual(saved.metrics.scopeMetrics,[]);
  const q=producer(t,file);await q.collect();await q.shutdown();
  assert.deepEqual(JSON.parse(fs.readFileSync(file,'utf8')).startTime,saved.startTime);
});
for(const [name,change] of [
  ['corrupt JSON',()=>'{'],
  ['older format',s=>JSON.stringify({...JSON.parse(s),version:0})],
  ['future format',s=>JSON.stringify({...JSON.parse(s),version:2})],
  ['invalid timestamp',s=>JSON.stringify({...JSON.parse(s),startTime:[0,-1]})],
  ['invalid metrics',s=>JSON.stringify({...JSON.parse(s),metrics:{}})],
  ['gauge metrics',s=>{const v=JSON.parse(s);v.metrics.scopeMetrics[0].metrics.find(m=>m.descriptor.name==='requests').dataPointType=2;return JSON.stringify(v);}],
  ['non-monotonic sums',s=>{const v=JSON.parse(s);v.metrics.scopeMetrics[0].metrics.find(m=>m.descriptor.name==='requests').isMonotonic=false;return JSON.stringify(v);}],
  ['changed point start',s=>{const v=JSON.parse(s);v.metrics.scopeMetrics[0].metrics[0].dataPoints[0].startTime=[1,0];return JSON.stringify(v);}],
  ['embedded different resource',s=>{const v=JSON.parse(s);v.metrics.resource.attributes['service.name']='wrong';return JSON.stringify(v);}]
]) test('rejects '+name+' without overwriting the file',async t=>{
  const file=folder(t),original=change(await seed(t,file));fs.writeFileSync(file,original);
  const diagnostics=[];assert.throws(()=>producer(t,file,{onDiagnostic:m=>diagnostics.push(m)}),/state|State|persist/i);
  assert.equal(diagnostics.length,1);assert.equal(fs.readFileSync(file,'utf8'),original);
});
for(const [name,options] of [
  ['resource',{resource:resourceFromAttributes({'service.name':'different'})}],
  ['resource schema',{resource:resourceFromAttributes({'service.name':'restart-test'},{schemaUrl:'other'})}],
  ['reducers',{gaugeReducers:{depth:'sum'}}],
  ['omissions',{omitResourceAttributes:['process.pid']}],
  ['limits',{maxSeries:1}],
  ['deadline',{timeoutMillis:123}]
]) test('rejects changed '+name+' configuration',async t=>{
  const file=folder(t),original=await seed(t,file);
  assert.throws(()=>producer(t,file,options),/configuration|state|State/);assert.equal(fs.readFileSync(file,'utf8'),original);
});
test('unreadable state fails construction with a diagnostic',t=>{
  const file=folder(t);fs.mkdirSync(file);const diagnostics=[];
  assert.throws(()=>producer(t,file,{onDiagnostic:m=>diagnostics.push(m)}));assert.equal(diagnostics.length,1);
});
test('write failure reports diagnostics and leaves collection usable',async t=>{
  const file=folder(t),bad=path.join(file,'absent','totals.json'),diagnostics=[];
  const p=producer(t,bad,{onDiagnostic:m=>diagnostics.push(m)});assert.equal((await p.collect()).errors.length,0);
  assert(diagnostics.some(m=>/persist|state/i.test(m)));await p.shutdown();assert(!fs.existsSync(bad));
});
test('a competing producer never overwrites another producer and diagnostics cannot throw',async t=>{
  const file=folder(t),diagnostics=[];
  const a=producer(t,file),b=producer(t,file,{onDiagnostic:m=>{diagnostics.push(m);throw new Error('observer');}});
  await a.collect();const first=fs.readFileSync(file,'utf8');
  await b.collect();await b.shutdown();assert.equal(fs.readFileSync(file,'utf8'),first);
  assert(diagnostics.some(m=>/another|changed|conflict/i.test(m)));
});
test('a busy writer lock is preserved and saving retries after it is removed',async t=>{
  const file=folder(t),diagnostics=[],p=producer(t,file,{onDiagnostic:m=>diagnostics.push(m)});
  fs.writeFileSync(file+'.lock','other writer');await p.collect();
  assert.equal(fs.readFileSync(file+'.lock','utf8'),'other writer');assert(!fs.existsSync(file));assert(diagnostics.length>0);
  fs.unlinkSync(file+'.lock');await p.collect();assert(fs.existsSync(file));
  assert.deepEqual(fs.readdirSync(path.dirname(file)),['totals.json']);
});
test('hard primary crash restores its last committed workers and loses only later unsent work', {timeout:15000},async t=>{
  const file=folder(t),a=await primary(t,file);await command(a,{command:'record',count:2});
  const first=await command(a,{command:'scrape'});
  await command(a,{command:'record',count:3});
  const exited=once(a,'exit');a.kill('SIGKILL');await exited;
  const b=await primary(t,file);await command(b,{command:'record',count:1});
  const next=await command(b,{command:'scrape'});
  assert.equal(metric(next,'requests').dataPoints[0].value,6);
  assert.deepEqual(metric(next,'requests').dataPoints[0].startTime,metric(first,'requests').dataPoints[0].startTime);
  const done=once(b,'exit');await command(b,{command:'stop'});await done;
});
test('shutdown saves worker finals received since the last scrape', {timeout:15000},async t=>{
  const file=folder(t),a=await primary(t,file);await command(a,{command:'record',count:2});
  const exited=once(a,'exit');await command(a,{command:'stop'});await exited;
  const b=await primary(t,file);const next=await command(b,{command:'scrape'});
  assert.equal(metric(next,'requests').dataPoints[0].value,4);
  assert.equal(metric(next,'latency').dataPoints[0].value.count,4);
  const done=once(b,'exit');await command(b,{command:'stop'});await done;
});
test('two primary processes cannot overwrite each other\'s checkpoint', {timeout:15000},async t=>{
  const file=folder(t),a=await primary(t,file),b=await primary(t,file);
  await command(a,{command:'record',count:2});await command(a,{command:'scrape'});
  const before=fs.readFileSync(file,'utf8');
  await command(b,{command:'record',count:5});const competing=await command(b,{command:'scrape'});
  assert.equal(metric(competing,'requests').dataPoints[0].value,10);
  assert(competing.diagnostics.some(m=>/another producer/.test(m)));assert.equal(fs.readFileSync(file,'utf8'),before);
  const stopped=once(b,'exit');await command(b,{command:'stop'});await stopped;
  assert.equal(fs.readFileSync(file,'utf8'),before);
  const done=once(a,'exit');await command(a,{command:'stop'});await done;
});
test('fsync and rename failures leave the previous checkpoint whole and clean temporary files',async t=>{
  const file=folder(t),diagnostics=[],p=producer(t,file,{onDiagnostic:m=>diagnostics.push(m)});await p.collect();
  const before=fs.readFileSync(file,'utf8');
  for(const operation of ['fsyncSync','renameSync']) {
    const original=fs[operation];let calls=0;
    const mocked=t.mock.method(fs,operation,(...args)=>{
      calls++;
      if(operation==='renameSync') {
        assert.equal(fs.readFileSync(file,'utf8'),before);
        assert.equal(JSON.parse(fs.readFileSync(args[0],'utf8')).version,1);
      }
      throw Object.assign(new Error('simulated I/O failure'),{code:'EIO'});
    });
    await p.collect();mocked.mock.restore();
    assert.equal(calls,1);assert.equal(fs.readFileSync(file,'utf8'),before);
    assert.deepEqual(fs.readdirSync(path.dirname(file)),['totals.json']);
    assert.equal(fs[operation],original);
  }
  assert.equal(diagnostics.length,2);await p.collect();assert.equal((await p.collect()).errors.length,0);
});
test('checkpoint decoding enforces maxSeries before restoring any state',async t=>{
  const file=folder(t),seeded=JSON.parse(await seed(t,file));fs.unlinkSync(file);
  const p=producer(t,file,{maxSeries:1});await p.collect();await p.shutdown();
  const limited=JSON.parse(fs.readFileSync(file,'utf8'));limited.metrics=seeded.metrics;limited.startTime=seeded.startTime;
  fs.writeFileSync(file,JSON.stringify(limited));
  assert.throws(()=>producer(t,file,{maxSeries:1}),/limit/);
});
test('a temporary filename collision never deletes an existing file',async t=>{
  const file=folder(t),p=producer(t,file);await p.collect();const before=fs.readFileSync(file,'utf8');
  const crypto=require('node:crypto'),fixed=t.mock.method(crypto,'randomUUID',()=> 'collision');
  const temp=`${file}.${process.pid}.collision.tmp`;fs.writeFileSync(temp,'belongs to someone else');
  await p.collect();fixed.mock.restore();
  assert.equal(fs.readFileSync(temp,'utf8'),'belongs to someone else');assert.equal(fs.readFileSync(file,'utf8'),before);
});

test('equivalent option ordering and explicit defaults restore the same checkpoint',async t=>{
  const file=folder(t),common=resourceFromAttributes({'service.name':'restart-test',region:'local'});
  const a=producer(t,file,{resource:common,gaugeReducers:{b:'min',a:'sum'},omitResourceAttributes:['pid','host']});
  await a.collect();await a.shutdown();
  const before=JSON.parse(fs.readFileSync(file,'utf8'));
  const b=producer(t,file,{resource:resourceFromAttributes({region:'local','service.name':'restart-test'}),
    gaugeReducers:{a:'sum',b:'min'},omitResourceAttributes:['host','pid','pid'],
    maxSeries:10000,maxWorkers:64,maxMessageBytes:1048576,timeoutMillis:5000});
  await b.collect();await b.shutdown();assert.deepEqual(JSON.parse(fs.readFileSync(file,'utf8')).startTime,before.startTime);
});
test('a leftover lock still permits restore and preserves the last complete checkpoint',async t=>{
  const file=folder(t),before=await seed(t,file),diagnostics=[];
  fs.writeFileSync(file+'.lock','crashed writer');fs.writeFileSync(file+'.leftover.tmp','partial');
  const p=producer(t,file,{onDiagnostic:m=>diagnostics.push(m)});
  const result=await p.collect();assert.equal(metric({metrics:result.resourceMetrics.scopeMetrics.flatMap(s=>s.metrics)},'requests').dataPoints[0].value,4);
  assert.equal(fs.readFileSync(file,'utf8'),before);assert(diagnostics.length>0);
  fs.unlinkSync(file+'.lock');await p.collect();assert.equal(JSON.parse(fs.readFileSync(file,'utf8')).version,1);
  assert.equal(fs.readFileSync(file+'.leftover.tmp','utf8'),'partial');
});
test('a submillisecond start timestamp is valid against the millisecond wall clock',async t=>{
  const file=folder(t),p=producer(t,file);await p.collect();await p.shutdown();
  const data=JSON.parse(fs.readFileSync(file,'utf8'));
  // A valid clock value can be partway through the millisecond returned by Date.now().
  data.startTime=[1700000000,500000];fs.writeFileSync(file,JSON.stringify(data));
  const clock=t.mock.method(Date,'now',()=>1700000000000);
  let q;
  try {q=producer(t,file);} finally {clock.mock.restore();}
  await q.collect();assert.deepEqual(JSON.parse(fs.readFileSync(file,'utf8')).startTime,data.startTime);
});
