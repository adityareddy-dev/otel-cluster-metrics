// Single-task workers: each one processes a batch, reports its result in band to the primary,
// then shuts its provider down so its final metrics reach the primary before it exits.
const cluster = require('node:cluster');
const { once } = require('node:events');
const assert = require('node:assert/strict');
const { MeterProvider } = require('@opentelemetry/sdk-metrics');
const { resourceFromAttributes } = require('@opentelemetry/resources');
const { WorkerMetricReader, ClusterMetricProducer } = require('../dist');
const resource = resourceFromAttributes({'service.name':'batch-demo'});

async function main() {
  if (cluster.isWorker) {
    const reader = new WorkerMetricReader();
    const provider = new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false});
    const processed = provider.getMeter('batch','1.0').createCounter('items_processed');
    const items = Number(process.env.ITEMS);
    for (let i = 0; i < items; i++) processed.add(1); // the task
    process.send({result:{items}}); // the in-band result is the primary's record of this task
    await provider.shutdown(); // the final snapshot is written before this resolves
    process.exit(0);
  }
  const producer = new ClusterMetricProducer({resource});
  let reported = 0;
  const workers = [3,4,5].map(items=>cluster.fork({ITEMS:String(items)}));
  for (const w of workers) w.on('message',m=>{ if (m.result) reported += m.result.items; });
  await Promise.all(workers.map(w=>once(w,'exit')));
  // Every worker is gone. The scrape still carries each task's counts.
  const result = await producer.collect();
  const metric = result.resourceMetrics.scopeMetrics[0].metrics.find(m=>m.descriptor.name === 'items_processed');
  const total = metric.dataPoints[0].value;
  assert.equal(reported,12); assert.equal(total,12);
  console.log(`In-band results: ${reported}. Scraped total after every worker exited: ${total}.`);
  await producer.shutdown();
}
main().catch(error=>{console.error(error);process.exitCode=1;});
