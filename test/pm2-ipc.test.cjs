const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createServer,connect} = require('node:net');
const {once} = require('node:events');
const {readLines} = require('../dist/pm2-ipc');

async function pair(t,maxBytes,onLine) {
  const server=createServer();
  server.listen(0,'127.0.0.1');await once(server,'listening');
  const accepted=once(server,'connection');
  const client=connect(server.address().port,'127.0.0.1');
  const [socket]=await accepted;
  t.after(async()=>{client.destroy();socket.destroy();await new Promise(r=>server.close(r));});
  const errors=[];socket.on('error',error=>errors.push(error));client.on('error',()=>{});
  readLines(socket,maxBytes,onLine);
  return {client,socket,errors};
}
test('PM2 framing handles split UTF-8 and multiple messages without changing the byte cap',async t=>{
  const lines=[];const {client}=await pair(t,16,line=>lines.push(line));
  const expected=['{"x":"é"}','{"y":1}'];const bytes=Buffer.from(expected.join('\n')+'\n');
  const done=new Promise(resolve=>{const wait=()=>lines.length===2?resolve():setImmediate(wait);wait();});
  // Split inside the UTF-8 character. Also exercise a message larger than the next write's remaining byte budget.
  client.write(bytes.subarray(0,7));client.write(bytes.subarray(7,12));client.write(bytes.subarray(12));
  await done;assert.deepEqual(lines,expected);
});
test('PM2 framing closes an oversized frame without accepting it',async t=>{
  const lines=[];const {client,socket,errors}=await pair(t,16,line=>lines.push(line));
  const closed=once(socket,'close');
  // events.once rejects on error, so observe close independently of the expected parser error.
  void closed.catch(()=>{});
  const ended=new Promise(resolve=>socket.once('close',resolve));
  client.write('x'.repeat(17));await ended;
  assert.deepEqual(lines,[]);assert.match(errors[0].message,/byte limit/);
});

test('a PM2 acknowledgement timeout fails the flush without filling the reliable pipe with retries',{timeout:10000},async t=>{
  const cluster=require('node:cluster');
  const path=require('node:path');
  const {tmpdir}=require('node:os');
  const {mkdtemp,rm}=require('node:fs/promises');
  const dir=await mkdtemp(path.join(process.env.OCM_PM2_TEST_HOME || tmpdir(),'pipe-'));
  const socketPath=path.join(dir,'reader.sock'),snapshots=[];const sockets=[];
  const server=createServer(socket=>{sockets.push(socket);socket.on('error',()=>{});readLines(socket,1024*1024,line=>snapshots.push(JSON.parse(line)));});
  let worker;
  t.after(async()=>{
    if(worker && !worker.isDead()) {const exited=once(worker,'exit');worker.kill('SIGKILL');await exited;}
    for(const socket of sockets) socket.destroy();await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});
  });
  server.listen(socketPath);await once(server,'listening');
  cluster.setupPrimary({exec:path.join(__dirname,'pm2-reader-worker.cjs')});
  worker=cluster.fork({METRICS_SOCKET:socketPath,name:'api',namespace:'default'});
  const [result]=await once(worker,'message');
  assert.match(result.flushError,/did not acknowledge/);
  assert.equal(snapshots.filter(s=>s.kind==='push').length,1);
});
