const cluster = require('node:cluster');
const { once } = require('node:events');
const { createServer } = require('node:http');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { MeterProvider, AggregationType } = require('@opentelemetry/sdk-metrics');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');
const { ClusterMetricProducer, WorkerMetricReader, createPrometheusHandler } = require('../dist');
const resource = resourceFromAttributes({'service.name':'restart-test'});
if (cluster.isWorker) {
  const provider = new MeterProvider({resource, readers:[new WorkerMetricReader({pushIntervalMillis:0})], sdkMetricsEnabled:false,
    views:[{instrumentName:'latency',aggregation:{type:AggregationType.EXPLICIT_BUCKET_HISTOGRAM,options:{boundaries:[10,20]}}}]});
  const meter = provider.getMeter('restart','1');
  const counter = meter.createCounter('requests'), histogram = meter.createHistogram('latency');
  meter.createGauge('depth').record(7);
  meter.createUpDownCounter('active').add(1);
  process.on('message', async m => {
    if (typeof m !== 'object' || !m.command) return;
    if (m.command === 'record') for (let i=0;i<m.count;i++) { counter.add(1); histogram.record(3); }
    if (m.command === 'flush') await provider.forceFlush();
    if (m.command === 'stop') { await provider.shutdown(); process.exit(0); }
    process.send({ack:m.id});
  });
  process.send({ready:true});
} else {
  (async()=>{
    const diagnostics=[];
    const producer = new ClusterMetricProducer({resource,stateFile:process.argv[2],onDiagnostic:m=>diagnostics.push(m)});
    const workers = Array.from({length:2},()=>cluster.fork());
    await Promise.all(workers.map(w=>once(w,'message')));
    let id=0;
    async function command(w, message) {
      const n=++id;
      const done=new Promise(resolve=>{ const f=m=>{if(m.ack===n){w.off('message',f);resolve();}}; w.on('message',f); });
      w.send({...message,id:n}); await done;
    }
    const exporter=new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
    const provider=new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});
    const server=createServer(createPrometheusHandler(exporter)); server.listen(0,'127.0.0.1'); await once(server,'listening');
    process.on('message',async m=>{
      try {
        let result;
        if (m.command==='record' || m.command==='flush') await Promise.all(workers.map(w=>command(w,m)));
        if (m.command==='scrape') {
          const response=await fetch(`http://127.0.0.1:${server.address().port}`);
          const body=await response.text();
          const collected=await producer.collect();
          result={status:response.status,body,diagnostics,metrics:collected.resourceMetrics.scopeMetrics.flatMap(s=>s.metrics)};
        }
        if (m.command==='stop') {
          await Promise.all(workers.map(w=>{const exited=once(w,'exit');w.send({command:'stop'});return exited;}));
          await new Promise(resolve=>server.close(resolve)); await provider.shutdown(); await producer.shutdown();
          process.send({ack:m.id},()=>process.exit(0)); return;
        }
        process.send({ack:m.id,result});
      } catch(e) {process.send({ack:m.id,error:e.message});}
    });
    process.send({ready:true});
  })().catch(e=>{console.error(e);process.exit(1);});
}
