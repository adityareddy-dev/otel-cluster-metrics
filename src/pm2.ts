import { randomUUID } from 'node:crypto';
import { createServer, type Server, type Socket } from 'node:net';
import type { EventEmitter } from 'node:events';
import { hrTime } from '@opentelemetry/core';
import type { CollectionResult, MetricCollectOptions, MetricProducer } from '@opentelemetry/sdk-metrics';
import type { ClusterMetricProducerOptions } from './index.js';
import { CHANNEL, VERSION, DEFAULT_MAX_BYTES, assert, positive, parseEnvelope, isOurMessage, decodeMetrics, resourceKey } from './protocol.js';
import { rowsOf, type WorkerState, type GaugeReducer } from './merge.js';
import { ClusterState } from './state.js';
import { readLines } from './pm2-ipc.js';

export interface Pm2MetricProducerOptions extends ClusterMetricProducerOptions {
  appName: string;
  namespace?: string;
  /** Local socket path shared with the app readers. An existing path is never removed. */
  socketPath: string;
  /** Same PM2_HOME as the app's daemon. Defaults to PM2's own default. */
  pm2Home?: string;
}
type Instance = {id:string;pid:number;bootId:string;socket:Socket};
type ProcessInfo = {pid:number;pm_id:number;name:string;pm2_env:{namespace:string;exec_mode:string;status:string;pm_uptime:number}};
// PM2's declarations omit its custom client. Keep this boundary local and load it only on connect.
type Client = {
  connect(cb:(error?:Error)=>void):void;
  list(cb:(error:Error|null,list:ProcessInfo[])=>void):void;
  disconnect(cb:()=>void):void;
  Client:{client_sock:EventEmitter & {set(name:string,value:number):void;retry:number}};
};
const reason=(error:unknown)=>error instanceof Error ? error.message : String(error);

/** One separate collector for one PM2 application and namespace. Keep it out of the app's reload group. */
export class Pm2MetricProducer implements MetricProducer {
  private readonly start=hrTime();
  private readonly state:ClusterState;
  private readonly instances=new Map<string,Instance>();
  private readonly sockets=new Set<Socket>();
  private readonly server:Server;
  private readonly timeout:number;
  private readonly maxBytes:number;
  private readonly maxSeries:number;
  private readonly maxWorkers:number;
  private readonly omit:string[];
  private readonly expectedResource:string;
  private readonly reducers:Record<string,GaugeReducer>;
  private readonly namespace:string;
  private pending?:Promise<CollectionResult>;
  private deferred:(()=>void)[]=[];
  private stopped=false;
  private failure?:Error;
  private revision=0;
  private roundMessage?:(instance:Instance,v:any,snapshot:WorkerState)=>void;
  private cancelRound?:(error:Error)=>void;
  private readonly operations=new Set<(error:Error)=>void>();
  private constructor(private readonly options:Pm2MetricProducerOptions,private readonly client:Client) {
    assert(typeof options.appName==='string' && options.appName.length>0,'Supply a PM2 appName');
    assert(typeof options.socketPath==='string' && options.socketPath.length>0,'Supply a PM2 socketPath');
    assert(options.namespace===undefined || typeof options.namespace==='string','Invalid PM2 namespace');
    assert(options.resource && !options.resource.asyncAttributesPending,'Supply a resolved common resource');
    this.namespace=options.namespace ?? 'default';
    this.timeout=positive(options.timeoutMillis ?? 5000,'timeoutMillis');
    this.maxBytes=positive(options.maxMessageBytes ?? DEFAULT_MAX_BYTES,'maxMessageBytes');
    this.maxSeries=positive(options.maxSeries ?? 10000,'maxSeries');
    this.maxWorkers=positive(options.maxWorkers ?? 64,'maxWorkers');
    this.omit=[...(options.omitResourceAttributes ?? [])];
    assert(this.omit.every(x=>typeof x==='string'),'Invalid resource attribute exclusions');
    this.expectedResource=resourceKey(options.resource,this.omit);
    this.reducers=Object.assign(Object.create(null),options.gaugeReducers ?? {});
    assert(Object.values(this.reducers).every(x=>['sum','min','max','mean'].includes(x)),'Invalid gauge reducer');
    assert(!('aggregationTemporality' in options),'Only cumulative aggregation is supported');
    this.state=new ClusterState(this.maxSeries,this.maxWorkers);
    this.server=createServer(this.onConnection);
  }
  static async connect(options:Pm2MetricProducerOptions):Promise<Pm2MetricProducer> {
    const PM2=require('pm2').custom;
    const client:Client=new PM2({pm2_home:options.pm2Home});
    const producer=new Pm2MetricProducer(options,client);
    try {
      await producer.deadline(new Promise<void>((resolve,reject)=>{
        producer.server.once('error',reject);
        producer.server.listen(options.socketPath,()=>{producer.server.off('error',reject);resolve();});
      }),producer.timeout,()=>{producer.server.close(()=>{});});
      producer.server.on('error',producer.onLost);
      await producer.deadline(new Promise<void>((resolve,reject)=>client.connect(error=>error?reject(error):resolve())),
        producer.timeout,()=>{client.disconnect(()=>{});});
      // Axon otherwise retries silently and hides a daemon restart from its consumer.
      client.Client.client_sock.set('retry timeout',0);client.Client.client_sock.retry=0;
      client.Client.client_sock.on('close',producer.onLost);client.Client.client_sock.on('error',producer.onLost);
      return producer;
    } catch(error) {await producer.shutdown();throw error;}
  }
  private deadline<T>(promise:Promise<T>,timeout=this.timeout,onLate?:()=>void):Promise<T> {
    return new Promise((resolve,reject)=>{
      let settled=false;
      const fail=(error:Error)=>{
        if(settled) return;settled=true;
        clearTimeout(timer);this.operations.delete(fail);reject(error);
      };
      const timer=setTimeout(()=>fail(new Error('PM2 operation timed out')),timeout);
      this.operations.add(fail);
      // A deadline cannot cancel PM2's work. Release resources it creates after cancellation,
      // even when shutdown already ran before its socket existed.
      promise.then(value=>{
        if(settled) {onLate?.();return;}settled=true;
        clearTimeout(timer);this.operations.delete(fail);resolve(value);
      },error=>{if(settled) onLate?.();else fail(error);});
    });
  }
  private diagnostic(message:string):void {try {this.options.onDiagnostic?.(message);} catch {}}
  private apply(fn:()=>void):void {try {fn();} catch(error) {this.diagnostic(reason(error));}}
  private later(fn:()=>void):void {if(this.pending) this.deferred.push(fn);else this.apply(fn);}
  private onLost=():void=>{
    if(this.stopped) return;
    this.failure=new Error('PM2 connection lost; restart the collector and app instances (totals reset)');
    this.cancelRound?.(this.failure);
    for(const fail of this.operations) fail(this.failure);
  };
  private onConnection=(socket:Socket):void=>{
    if(this.stopped || this.failure || this.sockets.size>=this.maxWorkers) {socket.destroy();return;}
    this.sockets.add(socket);
    let instance:Instance|undefined;
    socket.on('error',error=>this.diagnostic(`PM2 IPC: ${reason(error)}`));
    socket.once('close',()=>{
      this.sockets.delete(socket);
      if(instance) {
        this.instances.delete(instance.id);this.revision++;
        this.cancelRound?.(new Error('PM2 worker disconnected during collection'));
        this.later(()=>this.state.forget(instance!.id));
      }
    });
    readLines(socket,this.maxBytes,line=>{
      if(!instance) {
        const v=parseEnvelope(line,this.maxBytes);
        assert(v.kind==='hello' && v.appName===this.options.appName && v.namespace===this.namespace,'PM2 app or namespace mismatch');
        positive(v.workerId,'workerId');positive(v.pid,'pid');
        assert(typeof v.bootId==='string' && v.bootId.length>0 && v.bootId.length<=128,'Invalid boot ID');
        const id=String(v.workerId);
        assert(!this.instances.has(id),'Duplicate PM2 worker connection');
        instance={id,pid:v.pid,bootId:v.bootId,socket};this.instances.set(id,instance);this.revision++;
      } else this.onMessage(instance,line);
    });
  };
  private snapshot(instance:Instance,v:any):WorkerState {
    const id=instance.id;
    assert(v.bootId===instance.bootId && Number.isSafeInteger(v.sequence) && v.sequence>0 && Number.isSafeInteger(v.collectedAt) && v.collectedAt>0,`Invalid ${v.kind} envelope`);
    assert(v.error===undefined,`Worker ${id}: ${String(v.error)}`);
    const metrics=decodeMetrics(v.metrics,this.maxSeries);
    assert(resourceKey(metrics.resource,this.omit)===this.expectedResource,`Resource conflict from worker ${id}`);
    for(const scope of metrics.scopeMetrics) for(const metric of scope.metrics) for(const point of metric.dataPoints) {
      assert(point.endTime[0]*1000+point.endTime[1]/1e6<=v.collectedAt+1,'Point timestamp is after collection');
    }
    return {bootId:v.bootId,sequence:v.sequence,rows:rowsOf(metrics,`${id}:${v.bootId}`)};
  }
  private send(instance:Instance,data:Record<string,unknown>):void {
    instance.socket.write(JSON.stringify({channel:CHANNEL,version:VERSION,...data,targetPid:instance.pid})+'\n');
  }
  private onMessage(instance:Instance,raw:unknown):void {
    if(this.stopped || this.failure || !isOurMessage(raw)) return;
    let v:any;
    try {
      v=parseEnvelope(raw,this.maxBytes);
      const snapshot=this.snapshot(instance,v);
      assert(['reply','push','final'].includes(v.kind),'Invalid PM2 snapshot kind');
      if(v.kind==='reply' || v.kind==='final') this.roundMessage?.(instance,v,snapshot);
      if(v.kind==='push' || v.kind==='final') {
        this.later(()=>{if(!this.state.isFinished(instance.id)) this.state.accept(instance.id,snapshot,v.kind==='final');});
        // Acknowledge validated receipt. Like Node IPC, reconciliation errors go to onDiagnostic.
        this.send(instance,{kind:'ack',bootId:v.bootId,sequence:v.sequence});
      }
    } catch(error) {
      if(v?.kind==='reply') this.cancelRound?.(error instanceof Error?error:new Error(reason(error)));
      this.diagnostic(`Discarded PM2 snapshot: ${reason(error)}`);
      if(v?.kind==='push' || v?.kind==='final') this.send(instance,{kind:'ack',bootId:v.bootId,sequence:v.sequence,error:reason(error)});
    }
  }
  collect(options:MetricCollectOptions={}):Promise<CollectionResult> {
    if(this.stopped) return Promise.reject(new Error('Pm2MetricProducer is shutdown'));
    if(this.failure) return Promise.reject(this.failure);
    if(this.pending) return this.pending;
    let timeout=this.timeout;
    if(options.timeoutMillis!==undefined) timeout=Math.min(timeout,positive(options.timeoutMillis,'timeoutMillis'));
    this.pending=this.round(timeout).finally(()=>{
      this.pending=undefined;const queue=this.deferred;this.deferred=[];
      if(!this.stopped) for(const fn of queue) this.apply(fn);
    });return this.pending;
  }
  private async list(timeout:number):Promise<ProcessInfo[]> {
    const list=await this.deadline(new Promise<ProcessInfo[]>((resolve,reject)=>this.client.list((error,list)=>error?reject(error):resolve(list))),timeout,()=>{
      // PM2 list() can implicitly reconnect. Keep a healthy client after a slow round,
      // but never retain a connection completed after shutdown or daemon loss.
      if(this.stopped || this.failure) this.client.disconnect(()=>{});
    });
    const selected=list.filter(p=>p.name===this.options.appName && p.pm2_env.namespace===this.namespace);
    for(const p of selected) assert(p.pm2_env.exec_mode==='cluster_mode','PM2 app must use cluster mode');
    assert(selected.length<=this.maxWorkers,'Worker limit exceeded');
    assert(selected.every(p=>p.pm2_env.status==='online' || ['stopped','errored'].includes(p.pm2_env.status)),'PM2 membership is changing');
    return selected.filter(p=>p.pm2_env.status==='online');
  }
  private async round(timeout:number):Promise<CollectionResult> {
    const began=Date.now(),revision=this.revision;
    const workers=await this.list(timeout);
    assert(!this.stopped,'Pm2MetricProducer is shutdown');if(this.failure) throw this.failure;
    // PM2 hides old reload workers. Their open pipes prevent silently omitting them.
    const livePids=new Set(workers.map(w=>w.pid));
    for(const [id,instance] of this.instances) if(!this.state.isFinished(id)) assert(livePids.has(instance.pid),'PM2 membership is changing (old reload instance)');
    const replies=await this.request(workers,Math.max(1,timeout-(Date.now()-began)));
    const after=await this.list(Math.max(1,timeout-(Date.now()-began)));
    assert(!this.stopped,'Pm2MetricProducer is shutdown');if(this.failure) throw this.failure;
    const membership=(list:ProcessInfo[])=>list.map(w=>`${w.pm_id}:${w.pid}:${w.pm2_env.pm_uptime}`).sort().join(',');
    assert(revision===this.revision && membership(workers)===membership(after),'PM2 membership changed during collection');
    return {resourceMetrics:this.state.round(this.options.resource,replies,this.reducers,this.start,hrTime()),errors:[]};
  }
  private request(workers:ProcessInfo[],timeout:number):Promise<Map<string,WorkerState>> {
    const targets=workers.map(w=>{
      const instance=[...this.instances.values()].find(i=>i.pid===w.pid);
      assert(instance,`Missing PM2 worker connection: ${w.pm_id}`);return instance;
    }).filter(i=>!this.state.isFinished(i.id));
    if(!targets.length) return Promise.resolve(new Map());
    const requestId=randomUUID(),requestedAt=Date.now();
    return new Promise((resolve,reject)=>{
      const replies=new Map<string,WorkerState>();let done=false;
      const finish=(error?:Error)=>{
        if(done) return;done=true;clearTimeout(timer);this.roundMessage=undefined;this.cancelRound=undefined;
        if(error) reject(error);else resolve(replies);
      };
      const timer=setTimeout(()=>finish(new Error('Missing PM2 workers: '+targets.filter(w=>!replies.has(w.id)).map(w=>w.id).join(', '))),timeout);
      this.cancelRound=finish;
      this.roundMessage=(instance,v,snapshot)=>{
        if(!targets.includes(instance)) return;
        if(v.kind!=='final') {
          if(v.requestId!==requestId) {this.diagnostic('Discarded late PM2 reply');return;}
          assert(v.requestedAt===requestedAt && v.collectedAt>=requestedAt && v.collectedAt<=Date.now(),'Invalid PM2 reply envelope');
        }
        if(replies.has(instance.id)) {this.diagnostic('Discarded duplicate PM2 reply');return;}
        replies.set(instance.id,snapshot);if(replies.size===targets.length) finish();
      };
      for(const instance of targets) this.send(instance,{kind:'collect',requestId,requestedAt,timeoutMillis:timeout});
    });
  }
  get stateSize():{workers:number;series:number;retiredSeries:number} {return this.state.size;}
  async shutdown():Promise<void> {
    if(this.stopped) return;this.stopped=true;
    this.cancelRound?.(new Error('Pm2MetricProducer is shutdown'));
    for(const fail of this.operations) fail(new Error('Pm2MetricProducer is shutdown'));
    this.client.Client.client_sock?.off('close',this.onLost);this.client.Client.client_sock?.off('error',this.onLost);
    for(const socket of this.sockets) socket.destroy();
    this.deferred=[];this.instances.clear();this.state.clear();
    await new Promise<void>(resolve=>this.server.close(()=>resolve()));
    await new Promise<void>(resolve=>this.client.disconnect(resolve));
  }
}
