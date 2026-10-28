// A real SDK worker with the package reader, driven by commands from the test primary.
const { MeterProvider, AggregationType } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { WorkerMetricReader } = require('../dist');
const resource = resourceFromAttributes({'service.name':'test'});
// PUSH=default leaves pushIntervalMillis unset, so the shipped default applies.
const reader = new WorkerMetricReader(process.env.PUSH === 'default' ? {} : {pushIntervalMillis:Number(process.env.PUSH ?? 0)});
const provider = new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false,views:[{instrumentName:'latency',aggregation:{type:AggregationType.EXPLICIT_BUCKET_HISTOGRAM,options:{boundaries:[10,20]}}}]});
const meter = provider.getMeter('fixture','1');
const requests = meter.createCounter('requests');
meter.createHistogram('latency').record(5);
meter.createGauge('depth').record(10);
requests.add(Number(process.env.COUNT ?? 1));
process.on('message',async m=>{
  if (typeof m !== 'object' || !m.command) return;
  if (m.command === 'add') requests.add(m.value);
  if (m.command === 'flush') await provider.forceFlush();
  if (m.command === 'exit') { await provider.shutdown(); process.exit(0); }
  process.send({ack:m.id});
});
if (process.env.MODE === 'once') provider.shutdown().then(()=>process.exit(0)); // records, finishes and leaves before any scrape
else if (process.env.MODE === 'unref') process.channel.unref(); // nothing else keeps the loop alive, so beforeExit fires
else process.send({ready:true});
