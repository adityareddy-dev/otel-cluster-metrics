// Randomized worker lifecycles against the primary state machine: totals never move backward and always equal the model.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { ClusterState } = require('../dist/state');
const { rowsOf } = require('../dist/merge');
const { decodeMetrics } = require('../dist/protocol');
const resource=resourceFromAttributes({'service.name':'test'});
function payload(value,count,second) {
  const point=v=>({attributes:{},startTime:[1,0],endTime:[second,0],value:v});
  return {resource:{attributes:{'service.name':'test'}},scopeMetrics:[{scope:{name:'fixture',version:'1'},metrics:[
    {descriptor:{name:'requests',description:'requests',unit:'',valueType:1},aggregationTemporality:1,dataPointType:3,isMonotonic:true,dataPoints:[point(value)]},
    {descriptor:{name:'latency',description:'latency',unit:'',valueType:1},aggregationTemporality:1,dataPointType:0,dataPoints:[point({count,sum:count*5,buckets:{boundaries:[10,20],counts:[count,0,0]}})]},
    {descriptor:{name:'depth',description:'depth',unit:'',valueType:1},aggregationTemporality:1,dataPointType:2,dataPoints:[point(value)]}]}]};
}
function rng(seed) { return ()=>{ seed=seed+0x6D2B79F5|0; let t=Math.imul(seed^seed>>>15,1|seed); t=t+Math.imul(t^t>>>7,61|t)^t; return ((t^t>>>14)>>>0)/4294967296; }; }
function scenario(seed) {
  const random=rng(seed), pick=xs=>xs[Math.floor(random()*xs.length)];
  const state=new ClusterState(100,64), workers=new Map();
  let credited=0, creditedCount=0, boots=0, clock=1, lastTotal=0, rounds=0;
  const snap=(id,w)=>({bootId:w.bootId,sequence:++w.sequence,rows:rowsOf(decodeMetrics(payload(w.value,w.count,++clock),1000),`${id}:${w.bootId}`)});
  const start=id=>workers.set(id,{bootId:`boot-${++boots}`,sequence:0,value:0,count:0,reported:0,reportedCount:0,finished:false});
  const report=w=>{ w.reported=w.value; w.reportedCount=w.count; };
  const leave=id=>{ const w=workers.get(id); credited+=w.reported; creditedCount+=w.reportedCount; workers.delete(id); };
  for (let step=0;step<80;step++) {
    const ids=[...workers.keys()], live=ids.filter(id=>!workers.get(id).finished), done=ids.filter(id=>workers.get(id).finished);
    const op=pick(['start','start','inc','inc','inc','push','round','round','final','crash','exit','restart','disconnect']);
    if (op==='start') { const free=['0','1','2','3','4','5','6','7'].filter(id=>!workers.has(id)); if(free.length) start(pick(free)); }
    else if (op==='inc' && live.length) { const w=workers.get(pick(live)); w.value+=1+Math.floor(random()*5); w.count+=1; }
    else if (op==='push' && live.length) { const id=pick(live), w=workers.get(id); state.accept(id,snap(id,w)); report(w); }
    else if (op==='final' && live.length) { const id=pick(live), w=workers.get(id); state.accept(id,snap(id,w),true); report(w); w.finished=true; leave(id); workers.set(id,{...w,reported:0,reportedCount:0}); } // the final is credited at once; the exit adds nothing
    else if (op==='crash' && live.length) { const id=pick(live); state.forget(id); leave(id); }
    else if (op==='disconnect' && live.length) { const id=pick(live); state.gone(id); state.forget(id); leave(id); }
    else if (op==='exit' && done.length) { const id=pick(done); state.forget(id); leave(id); }
    else if (op==='restart' && ids.length) { const id=pick(ids); state.forget(id); leave(id); start(id); }
    else if (op==='round') {
      const replies=new Map(); for (const id of live) { const w=workers.get(id); replies.set(id,snap(id,w)); report(w); }
      const out=state.round(resource,replies,{},[0,0],[clock,0]), metrics=out.scopeMetrics.flatMap(s=>s.metrics);
      const total=metrics.find(m=>m.descriptor.name==='requests')?.dataPoints[0]?.value ?? 0;
      const count=metrics.find(m=>m.descriptor.name==='latency')?.dataPoints[0]?.value.count ?? 0;
      const expected=credited+live.reduce((n,id)=>n+workers.get(id).value,0);
      assert.equal(total,expected,`seed ${seed} step ${step}: total ${total}, expected ${expected}`);
      assert.equal(count,creditedCount+live.reduce((n,id)=>n+workers.get(id).count,0),`seed ${seed} step ${step}: histogram count`);
      assert(total>=lastTotal,`seed ${seed} step ${step}: total went backward ${lastTotal} to ${total}`);
      assert.equal(state.size.workers,live.length,`seed ${seed} step ${step}: live workers`);
      lastTotal=total; rounds++;
    }
    assert(state.size.retiredSeries<=2 && state.size.series<=2+3*live.length,`seed ${seed} step ${step}: state grew`);
  }
  return rounds;
}
test('randomized lifecycles: exits, restarts with the same id, pushes, finals, crashes and disconnects never move totals backward',()=>{
  let rounds=0; for (let seed=1;seed<=200;seed++) rounds+=scenario(seed);
  assert(rounds>1000,`only ${rounds} rounds`);
});
