const { resourceFromAttributes } = require('@opentelemetry/resources');
const { encodeMetrics } = require('../dist/protocol');
const CHANNEL = 'otel-cluster-metrics';
let sequence=0, bootId='boot-'+process.pid;
let state={counter:1,gauge:10,updown:2,count:1,sum:5,counts:[1,0,0],boundaries:[10,20],start:[1,0],resource:{'service.name':'test'},attrs:{},delay:0};
function metric(name,type,value,extra={}) {
  return {descriptor:{name,description:name,unit:'',valueType:1},aggregationTemporality:1,dataPointType:type,...extra,dataPoints:[{attributes:state.attrs,startTime:state.start,endTime:state.endTime ?? [Math.floor(Date.now()/1000),0],value}]};
}
function payload() {
  const metrics=state.empty ? [] : [metric('requests',3,state.counter,{isMonotonic:true}),metric('depth',2,state.gauge),metric('active',3,state.updown,{isMonotonic:false}),metric('latency',0,{count:state.count,sum:state.sum,min:5,max:5,buckets:{boundaries:state.boundaries,counts:state.counts}})];
  if(state.extra) metrics.push(...state.extra);
  if(state.temporality !== undefined) for(const m of metrics) m.aggregationTemporality=state.temporality;
  if(state.description) for(const m of metrics) m.descriptor.description=state.description;
  return encodeMetrics({resource:resourceFromAttributes(state.resource,{schemaUrl:state.schemaUrl}),scopeMetrics:[{scope:{name:'fixture',version:'1'},metrics}]});
}
function envelope(kind,extra={}) { return {channel:CHANNEL,version:state.version ?? 1,kind,...extra,bootId,sequence:state.stale ? 1 : ++sequence,collectedAt:Date.now(),metrics:payload()}; }
function emit(kind,counter,exit) { if(counter !== undefined) state.counter=counter; process.send(JSON.stringify(envelope(kind)),()=>{ if(exit) process.exit(0); }); }
process.on('message',raw=>{
  if (typeof raw !== 'string') {
    if(raw.command==='set') Object.assign(state,raw.state);
    if(raw.command==='emit') { emit(raw.kind,raw.counter,raw.exit); if(raw.exit) return; }
    process.send({ack:raw.id});return;
  }
  const req=JSON.parse(raw);
  if (req.channel !== CHANNEL || state.missing) return;
  if (state.exitOnCollect) process.exit(0);
  if (state.finalThenExit !== undefined) { emit('final',state.finalThenExit,true); return; }
  const send=()=>{
    const reply=envelope('reply',{requestId:req.requestId,requestedAt:req.requestedAt});
    if(state.wrongBoot) reply.bootId='';
    if(state.error) reply.error='fixture failure';
    const body=JSON.stringify(reply);
    process.send(state.oversize ? body+' '.repeat(5000) : body,()=>{ if(state.pushThenExit !== undefined) emit('push',state.pushThenExit,true); });
    if(state.duplicate) process.send(body);
  };
  setTimeout(send,state.delay);
});
process.send({ready:true});
