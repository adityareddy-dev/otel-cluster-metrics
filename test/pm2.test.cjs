const {test} = require('node:test');
const assert = require('node:assert/strict');
const {mkdtemp,rm} = require('node:fs/promises');
const {tmpdir} = require('node:os');
const path = require('node:path');
const {createServer} = require('node:http');
const {once} = require('node:events');
const {execFile,execFileSync} = require('node:child_process');
const {promisify} = require('node:util');
const {resourceFromAttributes} = require('@opentelemetry/resources');
const {MeterProvider,PeriodicExportingMetricReader} = require('@opentelemetry/sdk-metrics');
const {PrometheusExporter} = require('@opentelemetry/exporter-prometheus');
const {OTLPMetricExporter} = require('@opentelemetry/exporter-metrics-otlp-http');
const {Pm2MetricProducer,createPrometheusHandler} = require('../dist');
const resource=resourceFromAttributes({'service.name':'pm2-test'});
const call=(client,method,...args)=>new Promise((resolve,reject)=>client[method](...args,(e,r)=>e?reject(e):resolve(r)));
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function listen(server) {server.listen(0,'127.0.0.1');await once(server,'listening');return server.address().port;}
async function port() {const s=createServer();const p=await listen(s);await new Promise(r=>s.close(r));return p;}
async function setup(t,options={}) {
  const home=await mkdtemp(path.join(process.env.OCM_PM2_TEST_HOME || tmpdir(),'ocm-pm2-'));
  const clients=[]; const servers=[]; const paused=new Set(); const producers=[];const providers=[];
  t.after(async()=>{
    // Run even when a setup assertion or a collection fails. Never touch the default daemon.
    for(const pid of paused) {try {process.kill(pid,'SIGCONT');} catch {}}
    for(const s of servers) await new Promise(r=>s.close(r));
    for(const p of producers) await p.shutdown().catch(()=>{});
    for(const p of providers) await p.shutdown().catch(()=>{});
    for(const c of clients) c.disconnect();
    await promisify(execFile)(process.execPath,[require.resolve('pm2/bin/pm2'),'kill'],{env:{...process.env,PM2_HOME:home},timeout:15000});
    await rm(home,{recursive:true,force:true});
  });
  const PM2=require('pm2').custom;
  const client=new PM2({pm2_home:home});clients.push(client);await call(client,'connect');client.Client.client_sock.set('retry timeout',0);client.Client.client_sock.retry=0;
  assert.equal(typeof Pm2MetricProducer?.connect,'function','PM2 support must be exported');
  const producer=await Pm2MetricProducer.connect({resource,appName:'api',namespace:'metrics-test',pm2Home:home,socketPath:path.join(home,'metrics.sock'),timeoutMillis:700,...options});producers.push(producer);
  const exporter=new PrometheusExporter({preventServerStart:true,metricProducers:[producer]});
  const provider=new MeterProvider({resource,readers:[exporter],sdkMetricsEnabled:false});providers.push(provider);
  const metrics=createServer(createPrometheusHandler(exporter));servers.push(metrics);
  const metricsUrl=`http://127.0.0.1:${await listen(metrics)}`;
  const appPort=await port(), appUrl=`http://127.0.0.1:${appPort}`;
  const start=async(name='api',namespace='metrics-test',reader=true,push=0)=>{
    const selected=name==='api' && namespace==='metrics-test';
    const listenPort=selected ? appPort : await port();
    await call(client,'start',path.join(__dirname,'pm2-app.cjs'),{name,namespace,force:true,execMode:'cluster',instances:2,
      waitReady:true,listenTimeout:10000,killTimeout:8000,env:{PUSH:String(push),METRICS_SOCKET:selected && reader ? path.join(home,'metrics.sock') : '',APP_PORT:String(listenPort)}});
    const end=Date.now()+10000;
    while(Date.now()<end) {
      const apps=(await call(client,'list')).filter(p=>p.name===name && p.pm2_env.namespace===namespace && p.pm2_env.status==='online');
      if(apps.length===2) {try {await fetch(`http://127.0.0.1:${listenPort}/status`);return;} catch {}}
      await delay(25);
    }
    throw new Error(`PM2 app did not become ready: ${name}/${namespace}`);
  };
  await start('api','metrics-test',options.testReader!==false,options.testPush ?? 0);
  const scrape=async()=>{const r=await fetch(metricsUrl);const body=await r.text();assert.equal(r.status,200,body);return body;};
  const total=body=>Number(body.match(/^requests_total(?:\{[^\n]*\})? (\d+)$/m)?.[1] ?? 0);
  const traffic=async n=>{const pids=new Set();for(let i=0;i<n;i++){const r=await fetch(appUrl+'/work',{headers:{connection:'close'}});assert.equal(r.status,200);pids.add(await r.text());}return pids;};
  return {client,producer,providers,producers,servers,scrape,total,traffic,start,metricsUrl,appUrl,home,paused};
}

test('PM2 cluster traffic has one total across restarts, reloads, app isolation and OTLP', {timeout:60000}, async t=>{
  const x=await setup(t); const {client,producer,scrape,total,traffic}=x;
  assert.equal((await traffic(20)).size,2,'traffic must reach both real cluster instances');
  let body=await scrape();assert.equal(total(body),20);assert.equal(total(await scrape()),20);
  assert.match(body,/latency_count(?:\{[^\n]*\})? 20/);
  assert.equal(body.split('\n').filter(l=>/^depth\{/.test(l)).length,2);
  let apps=(await call(client,'list')).filter(p=>p.name==='api');
  await traffic(7); // no scrape before restart: final snapshot must retain these
  await call(client,'restart',apps[0].pm_id);
  assert.equal(total(await scrape()),27);
  await traffic(11);await call(client,'reload','api');
  assert.equal(total(await scrape()),38);assert.equal(total(await scrape()),38);
  assert.equal((await scrape()).split('\n').filter(l=>/^depth\{/.test(l)).length,2,'retired gauges disappear');
  await x.start('other'); await x.start('api','other-namespace');
  assert.equal(total(await scrape()),38,'other apps and namespaces must be excluded');
  apps=(await call(client,'list')).filter(p=>p.name==='api' && p.pm2_env.namespace==='metrics-test');
  assert.equal(apps.length,2,'original namespace still has two instances');
  x.paused.add(apps[0].pid);process.kill(apps[0].pid,'SIGSTOP');
  t.after(()=>{try {process.kill(apps[0].pid,'SIGCONT');} catch {}});
  const before=producer.stateSize;
  const bad=await fetch(x.metricsUrl);assert.equal(bad.status,503);assert.doesNotMatch(await bad.text(),/requests_total/);
  // Pushes can update live state, but the failed round must not retire any worker.
  assert.deepEqual(producer.stateSize,before);
  process.kill(apps[0].pid,'SIGCONT');
  assert.equal(total(await scrape()),38);
  const bodies=[];const receiver=createServer((req,res)=>{let raw='';req.on('data',c=>raw+=c);req.on('end',()=>{bodies.push(JSON.parse(raw));res.end('{}');});});x.servers.push(receiver);
  const receiverPort=await listen(receiver);
  const reader=new PeriodicExportingMetricReader({exporter:new OTLPMetricExporter({url:`http://127.0.0.1:${receiverPort}/v1/metrics`}),exportIntervalMillis:600000,metricProducers:[producer]});
  const provider=new MeterProvider({resource,readers:[reader],sdkMetricsEnabled:false});x.providers.push(provider);
  await provider.forceFlush();assert.equal(bodies.length,1);
  const metric=bodies[0].resourceMetrics.flatMap(r=>r.scopeMetrics).flatMap(s=>s.metrics).find(m=>m.name==='requests');
  assert.equal(metric.sum.dataPoints.length,1);assert.equal(Number(metric.sum.dataPoints[0].asInt ?? metric.sum.dataPoints[0].asDouble),38);
  await call(client,'delete','all');
  assert.equal(total(await scrape()),38);assert.equal(producer.stateSize.workers,0);
  assert.equal(producer.stateSize.retiredSeries,2);
  const pending=producer.collect();assert.strictEqual(producer.collect(),pending);
  const rejected=assert.rejects(pending,/shutdown/);await producer.shutdown();await rejected;await assert.rejects(producer.collect(),/shutdown/);
});


test('the Node cluster entry point loads without the optional PM2 package',()=>{
  const script="const Module=require('node:module');const load=Module._load;Module._load=function(name,...args){if(name==='pm2')throw Error('PM2 must stay optional');return load.call(this,name,...args);};require('./dist');";
  execFileSync(process.execPath,['-e',script],{cwd:path.join(__dirname,'..'),timeout:5000});
});

test('PM2 instances without readers fail closed instead of exporting an empty total',{timeout:20000},async t=>{
  const x=await setup(t,{testReader:false});
  await x.traffic(5);
  const r=await fetch(x.metricsUrl);assert.equal(r.status,503);assert.doesNotMatch(await r.text(),/requests_total/);
  assert.deepEqual(x.producer.stateSize,{workers:0,series:0,retiredSeries:0});
});

test('PM2 hard kills keep acknowledged totals and replacement epochs count once',{timeout:25000},async t=>{
  const x=await setup(t);
  await x.traffic(10);
  // The scrape commits the checkpoint that must survive the hard kill.
  assert.equal(x.total(await x.scrape()),10);
  const [old]=(await call(x.client,'list')).filter(p=>p.name==='api');
  process.kill(old.pid,'SIGKILL');
  const end=Date.now()+10000;
  while(Date.now()<end) {
    const list=(await call(x.client,'list')).filter(p=>p.name==='api');
    if(list.length===2 && list.every(p=>p.pid && p.pid!==old.pid && p.pm2_env.status==='online')) {
      try {if(x.total(await x.scrape())===10) break;} catch {}
    }
    await delay(25);
  }
  assert.equal(x.total(await x.scrape()),10);
  await x.traffic(6);assert.equal(x.total(await x.scrape()),16);
  assert(x.producer.stateSize.series<=8,'state must retain only live snapshots and folded series');
});

test('losing the PM2 daemon invalidates the collector instead of reconnecting to empty history',{timeout:20000},async t=>{
  const x=await setup(t);await x.traffic(4);assert.equal(x.total(await x.scrape()),4);
  await call(x.client,'killDaemon');
  await assert.rejects(x.producer.collect(),/PM2 connection lost/);
});


test('PM2 reload overlap fails scrapes until draining old instances send their finals',{timeout:25000},async t=>{
  const x=await setup(t);await x.traffic(4);assert.equal(x.total(await x.scrape()),4);
  const oldPids=new Set((await call(x.client,'list')).map(p=>p.pid));
  const held=await Promise.all([0,1].map(()=>fetch(x.appUrl+'/hold',{headers:{connection:'close'}})));
  const reload=call(x.client,'reload','api');
  const end=Date.now()+10000;
  let overlapped=false;
  while(Date.now()<end) {
    const list=await call(x.client,'list');
    if(list.some(p=>p.pid && !oldPids.has(p.pid))) {overlapped=true;break;}
    await delay(10);
  }
  assert(overlapped,'replacement must have started while old requests drain');
  const r=await fetch(x.metricsUrl);assert.equal(r.status,503);assert.doesNotMatch(await r.text(),/requests_total/);
  await Promise.all(held.map(r=>r.text()));await reload;
  assert.equal(x.total(await x.scrape()),6);assert.equal(x.total(await x.scrape()),6);
});
