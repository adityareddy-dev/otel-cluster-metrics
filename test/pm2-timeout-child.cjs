const assert = require('node:assert/strict');
const path = require('node:path');
const childProcess = require('node:child_process');
const spawn = childProcess.spawn;
const stdio = new Set([process.stdin,process.stdout,process.stderr]);
childProcess.spawn = (...args)=>{
  const child=spawn(...args);
  // PM2 leaves its detached daemon's write-only stdin pipe in the parent.
  // Like our own stdio, that pipe does not keep the collector running.
  for(const stream of child.stdio) if(stream) stdio.add(stream);
  return child;
};
const mode = process.argv[2] || 'connect';
let listCompleted;
const lateList = new Promise(resolve=>{listCompleted=resolve;});
if(mode==='listen') {
  const net = require('node:net');
  const createServer = net.createServer;
  net.createServer = (...args)=>{
    const server=createServer(...args),listen=server.listen;
    server.listen = (...args)=>{
      const cb=args.pop();
      setTimeout(()=>listen.call(server,...args,()=>{
        console.log('Socket listen completed');cb();
      }),50);
      return server;
    };
    return server;
  };
}
const pm2 = require('pm2');
const RealClient = pm2.custom;
pm2.custom = class extends RealClient {
  connect(cb) {
    return super.connect(error=>{
      console.log('PM2 connect completed');
      cb(mode==='connect-error' ? new Error('Late connection failure') : error);
    });
  }
  list(cb) {
    this.listCalls=(this.listCalls || 0)+1;
    if(this.listCalls!==({'list-before':1,'list-after':2,'list-recover':1}[mode])) return super.list(cb);
    // Dispatch after the deadline to exercise PM2's real implicit RPC reconnect.
    setTimeout(()=>super.list((...args)=>{
      console.log('PM2 list completed');cb(...args);listCompleted();
    }),50);
  }
};
const {Pm2MetricProducer} = require('../dist');
const {resourceFromAttributes} = require('@opentelemetry/resources');

process.on('beforeExit',()=>{
  const handles=process._getActiveHandles().filter(h=>
    !stdio.has(h) && !h.destroyed);
  assert.deepEqual(handles.map(h=>h.constructor.name),[], 'collector must release its handles');
  console.log('No open collector handles');
});

(async()=>{
  const starting=Pm2MetricProducer.connect({
    resource:resourceFromAttributes({}),appName:'api',pm2Home:process.env.PM2_HOME,
    socketPath:path.join(process.env.PM2_HOME,'metrics.sock'),
    timeoutMillis:mode.startsWith('list-') ? 5000 : 10,
  });
  if(mode.startsWith('list-')) {
    const producer=await starting;
    try {
      await assert.rejects(producer.collect({timeoutMillis:10}),/PM2 operation timed out/);
      console.log('Collection rejected');
      if(mode==='list-recover') {
        await lateList;
        assert.deepEqual((await producer.collect()).errors,[]);
        console.log('Collection recovered');
      }
    } finally {await producer.shutdown();}
  } else {
    await assert.rejects(starting,/PM2 operation timed out/);
    console.log('Startup rejected');
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
