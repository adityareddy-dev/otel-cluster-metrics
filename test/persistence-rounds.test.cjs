const {test}=require('node:test');
const assert=require('node:assert/strict');
const cluster=require('node:cluster');
const {once}=require('node:events');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {resourceFromAttributes}=require('@opentelemetry/resources');
const {ClusterMetricProducer}=require('../dist');
const resource=resourceFromAttributes({'service.name':'test'});
cluster.setupPrimary({exec:path.join(__dirname,'worker.cjs')});
let commandId=0;
async function send(worker,message) {
  const id=++commandId;
  const done=new Promise(resolve=>{const f=m=>{if(m.ack===id){worker.off('message',f);resolve();}};worker.on('message',f);});
  worker.send({...message,id});await done;
}
async function setup(t,options={}) {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'ocm-round-')),file=path.join(dir,'totals.json'),diagnostics=[];
  const p=new ClusterMetricProducer({resource,stateFile:file,timeoutMillis:1000,onDiagnostic:m=>diagnostics.push(m),...options});
  const worker=cluster.fork();await once(worker,'message');
  t.after(async()=>{if(!worker.isDead()){const exited=once(worker,'exit');worker.kill('SIGKILL');await exited;}await p.shutdown();fs.rmSync(dir,{recursive:true,force:true});});
  return {p,worker,file,diagnostics};
}
function saved(file,name='requests') {
  return JSON.parse(fs.readFileSync(file,'utf8')).metrics.scopeMetrics.flatMap(s=>s.metrics).find(m=>m.descriptor.name===name).dataPoints[0].value;
}
test('failed rounds never checkpoint a candidate reply and the next round commits once',async t=>{
  const {p,worker,file}=await setup(t);await p.collect();const before=fs.readFileSync(file,'utf8');
  await send(worker,{command:'set',state:{counter:9,counts:[0,0,0]}});
  await assert.rejects(p.collect(),/histogram/);assert.equal(fs.readFileSync(file,'utf8'),before);
  await send(worker,{command:'set',state:{counts:[1,0,0]}});await p.collect();assert.equal(saved(file),9);
  await p.collect();assert.equal(saved(file),9);
});
test('pushes, rejected pushes and worker exits checkpoint only committed cumulative values',async t=>{
  const {p,worker,file,diagnostics}=await setup(t);await p.collect();
  await send(worker,{command:'emit',kind:'push',counter:7});assert.equal(saved(file),7);
  await send(worker,{command:'emit',kind:'push',counter:2});assert.equal(saved(file),7);
  assert(diagnostics.some(m=>/decreased/.test(m)));
  const exited=once(worker,'exit');worker.kill('SIGKILL');await exited;
  await p.collect();assert.equal(saved(file),7);assert.equal(p.stateSize.workers,0);
  assert.deepEqual(JSON.parse(fs.readFileSync(file,'utf8')).metrics.scopeMetrics.flatMap(s=>s.metrics.map(m=>m.descriptor.name)).sort(),['latency','requests']);
});
test('shutdown cancels a round without checkpointing its uncommitted replies',async t=>{
  const {p,worker,file}=await setup(t);await p.collect();
  await send(worker,{command:'set',state:{counter:9,delay:100}});
  const round=assert.rejects(p.collect(),/shutdown/);await p.shutdown();await round;assert.equal(saved(file),1);
});
test('restored totals count toward maxSeries and a rejected new series leaves the checkpoint intact',async t=>{
  const {p,worker,file}=await setup(t,{maxSeries:4});await p.collect();await p.shutdown();
  const q=new ClusterMetricProducer({resource,stateFile:file,timeoutMillis:1000,maxSeries:4});t.after(()=>q.shutdown());
  assert.deepEqual(q.stateSize,{workers:0,series:2,retiredSeries:2});
  const before=fs.readFileSync(file,'utf8');await assert.rejects(q.collect(),/series limit/);
  assert.deepEqual(q.stateSize,{workers:0,series:2,retiredSeries:2});assert.equal(fs.readFileSync(file,'utf8'),before);
});
test('a failed replacement keeps the old checkpoint while collection commits and later retries',async t=>{
  const {p,worker,file,diagnostics}=await setup(t);await p.collect();
  await send(worker,{command:'set',state:{counter:9}});
  const fail=t.mock.method(fs,'renameSync',(temp,target)=>{
    assert.equal(target,file);assert.equal(saved(file),1);assert.equal(saved(temp),9);
    throw Object.assign(new Error('disk failure'),{code:'EIO'});
  });
  const collected=await p.collect();fail.mock.restore();
  assert.equal(collected.resourceMetrics.scopeMetrics.flatMap(s=>s.metrics).find(m=>m.descriptor.name==='requests').dataPoints[0].value,9);
  assert.equal(saved(file),1);assert(diagnostics.some(m=>/disk failure/.test(m)));
  assert.deepEqual(fs.readdirSync(path.dirname(file)),['totals.json']);
  await p.collect();assert.equal(saved(file),9);
});
