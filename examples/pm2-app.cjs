const {createServer} = require('node:http');
const {MeterProvider} = require('@opentelemetry/sdk-metrics');
const {resourceFromAttributes} = require('@opentelemetry/resources');
const {WorkerMetricReader} = require('../dist');

const resource=resourceFromAttributes({'service.name':'pm2-api'});
const reader=new WorkerMetricReader({transport:'pm2',pm2SocketPath:process.env.METRICS_SOCKET});
const provider=new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false});
const requests=provider.getMeter('pm2-api').createCounter('requests');
const server=createServer((_req,res)=>{requests.add(1);res.end('ok\n');});

async function shutdown() {
  await new Promise(resolve=>server.close(resolve));
  try {await provider.shutdown();process.exit(0);} catch(error) {console.error(error);process.exit(1);}
}
process.once('SIGINT',shutdown);
process.once('SIGTERM',shutdown);
// Register and confirm the collector received this instance before accepting traffic.
provider.forceFlush().then(()=>{
  server.listen(Number(process.env.PORT ?? 3000),()=>process.send('ready'));
}).catch(error=>{console.error(error);process.exit(1);});
