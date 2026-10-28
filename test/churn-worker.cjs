// A short-lived worker that speaks the wire format directly, so hundreds of them start fast.
const now=Math.floor(Date.now()/1000);
const point=value=>({attributes:{},startTime:[1,0],endTime:[now,0],value});
const metrics={resource:{attributes:{'service.name':'test'}},scopeMetrics:[{scope:{name:'fixture',version:'1'},metrics:[
  {descriptor:{name:'requests',description:'requests',unit:'',valueType:1},aggregationTemporality:1,dataPointType:3,isMonotonic:true,dataPoints:[point(1)]},
  {descriptor:{name:'latency',description:'latency',unit:'',valueType:1},aggregationTemporality:1,dataPointType:0,dataPoints:[point({count:1,sum:5,min:5,max:5,buckets:{boundaries:[10,20],counts:[1,0,0]}})]},
  {descriptor:{name:'depth',description:'depth',unit:'',valueType:1},aggregationTemporality:1,dataPointType:2,dataPoints:[point(10)]}]}]};
if (process.env.FINAL === '1') process.send(JSON.stringify({channel:'otel-cluster-metrics',version:1,kind:'final',bootId:'boot-'+process.pid,sequence:1,collectedAt:Date.now(),metrics}),()=>process.exit(0));
else process.exit(0);
