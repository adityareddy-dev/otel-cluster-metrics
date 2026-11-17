const {MeterProvider} = require('@opentelemetry/sdk-metrics');
const {resourceFromAttributes} = require('@opentelemetry/resources');
const {WorkerMetricReader} = require('../dist');
const reader=new WorkerMetricReader({transport:'pm2',pm2SocketPath:process.env.METRICS_SOCKET,pushIntervalMillis:0,ackTimeoutMillis:300});
const provider=new MeterProvider({resource:resourceFromAttributes({'service.name':'pipe-test'}),readers:[reader],sdkMetricsEnabled:false});
provider.getMeter('pipe-test').createCounter('requests').add(1);
(async()=>{
  try {await provider.forceFlush();process.send({flushed:true});}
  catch(error) {process.send({flushError:error.message});}
  await provider.shutdown().catch(()=>{});process.exit(0);
})();
