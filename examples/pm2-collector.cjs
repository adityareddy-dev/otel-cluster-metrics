const {createServer} = require('node:http');
const {MeterProvider} = require('@opentelemetry/sdk-metrics');
const {resourceFromAttributes} = require('@opentelemetry/resources');
const {PrometheusExporter} = require('@opentelemetry/exporter-prometheus');
const {Pm2MetricProducer,createPrometheusHandler} = require('../dist');

async function main() {
  const resource=resourceFromAttributes({'service.name':'pm2-api'});
  const producer=await Pm2MetricProducer.connect({
    resource,appName:'api',socketPath:process.env.METRICS_SOCKET,pm2Home:process.env.PM2_HOME,
    onDiagnostic:message=>console.error(message),
  });
  const exporter=new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
  const provider=new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});
  const handler=createPrometheusHandler(exporter);
  const server=createServer((req,res)=>{
    if(req.url==='/metrics') void handler(req,res);
    else {res.writeHead(404);res.end();}
  });
  server.listen(Number(process.env.METRICS_PORT ?? 9464),'127.0.0.1',()=>console.log(`PM2 metrics on http://127.0.0.1:${server.address().port}/metrics`));
  const shutdown=async()=>{
    await new Promise(resolve=>server.close(resolve));
    await provider.shutdown();await producer.shutdown();
  };
  process.once('SIGINT',()=>{void shutdown();});
  process.once('SIGTERM',()=>{void shutdown();});
}
main().catch(error=>{console.error(error);process.exitCode=1;});
