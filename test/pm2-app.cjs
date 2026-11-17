const {createServer} = require('node:http');
const {MeterProvider, AggregationType} = require('@opentelemetry/sdk-metrics');
const {resourceFromAttributes} = require('@opentelemetry/resources');
const {WorkerMetricReader} = require('../dist');
const reader = process.env.METRICS_SOCKET ? new WorkerMetricReader({transport:'pm2',pushIntervalMillis:Number(process.env.PUSH ?? 0),ackTimeoutMillis:3000,pm2SocketPath:process.env.METRICS_SOCKET}) : undefined;
const provider = new MeterProvider({resource:resourceFromAttributes({'service.name':'pm2-test'}),readers:reader?[reader]:[],sdkMetricsEnabled:false,
  views:[{instrumentName:'latency',aggregation:{type:AggregationType.EXPLICIT_BUCKET_HISTOGRAM,options:{boundaries:[10,20]}}}]});
const meter=provider.getMeter('pm2-test');
const requests=meter.createCounter('requests'), latency=meter.createHistogram('latency');
meter.createObservableGauge('depth').addCallback(r=>r.observe(1));
const server=createServer(async(req,res)=>{
  if(req.url==='/hold') {requests.add(1);latency.record(5);res.writeHead(200);res.flushHeaders();setTimeout(()=>res.end(String(process.pid)),2000);}
  else if(req.url==='/work') {requests.add(1);latency.record(5);res.end(String(process.pid));}
  else if(req.url==='/flush') {await provider.forceFlush();res.end('flushed');}
  else {res.statusCode=404;res.end();}
});
async function shutdown() {
  await new Promise(resolve=>server.close(resolve));
  try {await provider.shutdown();process.exit(0);} catch(error) {console.error(error);process.exit(1);}
}
process.once('SIGINT',shutdown);
process.once('SIGTERM',shutdown);
provider.forceFlush().then(()=>server.listen(Number(process.env.APP_PORT),'127.0.0.1',()=>process.send('ready'))).catch(e=>{console.error(e);process.exit(1);});
