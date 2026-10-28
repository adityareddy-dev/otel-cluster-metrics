import cluster, { type Worker } from 'node:cluster';
import { randomUUID } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { diag } from '@opentelemetry/api';
import { hrTime } from '@opentelemetry/core';
import type { Resource } from '@opentelemetry/resources';
import { PrometheusExporter, PrometheusSerializer } from '@opentelemetry/exporter-prometheus';
import { AggregationTemporality, MetricReader, type CollectionResult, type MetricCollectOptions, type MetricProducer } from '@opentelemetry/sdk-metrics';
import { CHANNEL, VERSION, DEFAULT_MAX_BYTES, assert, positive, parseEnvelope, isOurMessage, encodeMetrics, decodeMetrics, resourceKey } from './protocol.js';
import { rowsOf, type GaugeReducer, type WorkerState } from './merge.js';
import { ClusterState } from './state.js';
export type { GaugeReducer } from './merge.js';

/** Milliseconds between unrequested worker snapshots. A crash then loses at most this much, not a scrape interval. */
export const DEFAULT_PUSH_INTERVAL_MILLIS = 10000;
const reason = (error: unknown): string => error instanceof Error ? error.message : String(error);

export interface WorkerMetricReaderOptions {
  maxMessageBytes?: number;
  maxPoints?: number;
  /** Milliseconds between unrequested snapshots to the primary. 0 turns the push off. */
  pushIntervalMillis?: number;
}
/** Register before recording metrics. Only Node cluster workers are supported. */
export class WorkerMetricReader extends MetricReader {
  private readonly bootId = randomUUID();
  private sequence = 0;
  private queue: Promise<void> = Promise.resolve();
  private stopped = false;
  private finalSent = false;
  private timer?: NodeJS.Timeout;
  private readonly maxBytes: number;
  private readonly maxPoints: number;
  constructor(options: WorkerMetricReaderOptions = {}) {
    super({aggregationTemporalitySelector:()=>AggregationTemporality.CUMULATIVE});
    assert(cluster.isWorker && process.send, 'WorkerMetricReader requires a Node cluster worker');
    assert(!('aggregationTemporalitySelector' in options) && !('metricProducers' in options) && !('aggregationTemporality' in options), 'Worker reader configuration is cumulative only');
    this.maxBytes = positive(options.maxMessageBytes ?? DEFAULT_MAX_BYTES,'maxMessageBytes');
    this.maxPoints = positive(options.maxPoints ?? 10000,'maxPoints');
    const interval = options.pushIntervalMillis ?? DEFAULT_PUSH_INTERVAL_MILLIS;
    assert(Number.isSafeInteger(interval) && interval >= 0, 'Invalid pushIntervalMillis');
    process.on('message',this.onMessage);
    process.on('beforeExit',this.onBeforeExit);
    // The timer never keeps a finished worker alive.
    if (interval > 0) this.timer = setInterval(()=>{ void this.snapshot('push'); },interval).unref();
  }
  private onMessage = (raw: unknown): void => {
    if (!isOurMessage(raw) || this.stopped) return;
    let request: any;
    try {
      request = parseEnvelope(raw,this.maxBytes);
      assert(request.kind === 'collect' && typeof request.requestId === 'string' && request.requestId.length <= 128 && Number.isSafeInteger(request.requestedAt) && request.requestedAt > 0, 'Invalid collection request');
      positive(request.timeoutMillis,'timeoutMillis');
    } catch { return; }
    void this.enqueue(()=>this.reply(request));
  };
  private onBeforeExit = (): void => {
    // Reached only when the application unreferenced the IPC channel, which otherwise keeps a worker alive.
    if (this.stopped || this.finalSent || !process.connected) return;
    void this.snapshot('final').then(()=>this.stop());
  };
  /** One SDK collection at a time, in send order, so sequence numbers follow the order on the pipe. */
  private enqueue(task: ()=>Promise<void>): Promise<void> {
    const run = this.queue.then(task);
    this.queue = run.catch(()=>{});
    return run;
  }
  private async encoded(timeoutMillis?: number): Promise<unknown> {
    const result = await this.collect({timeoutMillis});
    assert(!result.errors.length, 'Worker SDK collection failed');
    const metrics = encodeMetrics(result.resourceMetrics);
    decodeMetrics(metrics,this.maxPoints);
    return metrics;
  }
  private async reply(request: any): Promise<void> {
    if (this.stopped) return;
    const head = {kind:'reply',requestId:request.requestId,requestedAt:request.requestedAt};
    try { await this.send(head,await this.encoded(request.timeoutMillis)); }
    catch (error) { await this.send(head,undefined,reason(error)); }
  }
  /** An unrequested snapshot: the periodic push, a forceFlush, or the final one before the reader stops. */
  private snapshot(kind: 'push' | 'final'): Promise<void> {
    return this.enqueue(async ()=>{
      if (this.stopped || this.finalSent || !process.connected) return;
      try {
        const metrics = await this.encoded();
        if (kind === 'final') this.finalSent = true;
        await this.send({kind},metrics);
      } catch (error) { diag.warn(`otel-cluster-metrics: ${kind} snapshot not sent: ${reason(error)}`); }
    });
  }
  /** Resolves once the message is written, so a caller may exit right after. */
  private send(head: Record<string,unknown>, metrics?: unknown, error?: string): Promise<void> {
    if (!process.connected || this.stopped) return Promise.resolve();
    const base = {channel:CHANNEL,version:VERSION,...head,bootId:this.bootId,sequence:++this.sequence,collectedAt:Date.now()};
    let body = JSON.stringify({...base,metrics,error});
    if (Buffer.byteLength(body) > this.maxBytes) body = JSON.stringify({...base,metrics:undefined,error:'IPC envelope exceeds byte limit'});
    if (Buffer.byteLength(body) > this.maxBytes) return Promise.resolve();
    return new Promise(resolve=>{ try { process.send!(body,()=>resolve()); } catch { resolve(); } });
  }
  private stop(): void { this.stopped=true; clearInterval(this.timer); process.off('message',this.onMessage); process.off('beforeExit',this.onBeforeExit); }
  /** Sends the final snapshot before stopping, so provider.shutdown() in a worker hands its last totals to the primary. */
  protected async onShutdown(): Promise<void> { await this.snapshot('final'); this.stop(); }
  /** Pushes a snapshot now, so provider.forceFlush() bounds the loss before a risky step. */
  protected async onForceFlush(): Promise<void> { await this.snapshot('push'); }
}

export interface ClusterMetricProducerOptions {
  /** Must match the primary provider's common service resource. */
  resource: Resource;
  /** Attributes explicitly removed from both worker and expected resources. */
  omitResourceAttributes?: string[];
  gaugeReducers?: Record<string,GaugeReducer>;
  timeoutMillis?: number;
  maxMessageBytes?: number;
  maxSeries?: number;
  maxWorkers?: number;
  /** Reports discarded duplicate/late messages, rejected pushes and malformed protocol data. */
  onDiagnostic?: (message: string) => void;
}
/** A cumulative producer for one primary process and one common resource. */
export class ClusterMetricProducer implements MetricProducer {
  private readonly start = hrTime();
  private readonly state: ClusterState;
  private pending?: Promise<CollectionResult>;
  private deferred: (()=>void)[] = [];
  private stopped = false;
  private cancelRound?: (error: Error)=>void;
  private readonly timeout: number;
  private readonly maxBytes: number;
  private readonly maxSeries: number;
  private readonly maxWorkers: number;
  private readonly expectedResource: string;
  private readonly omit: string[];
  private readonly reducers: Record<string,GaugeReducer>;
  constructor(private readonly options: ClusterMetricProducerOptions) {
    assert(cluster.isPrimary, 'ClusterMetricProducer requires the primary process');
    assert(options.resource && !options.resource.asyncAttributesPending,'Supply a resolved common resource');
    this.timeout=positive(options.timeoutMillis ?? 5000,'timeoutMillis');
    this.maxBytes=positive(options.maxMessageBytes ?? DEFAULT_MAX_BYTES,'maxMessageBytes');
    this.maxSeries=positive(options.maxSeries ?? 10000,'maxSeries');
    this.maxWorkers=positive(options.maxWorkers ?? 64,'maxWorkers');
    this.omit=[...(options.omitResourceAttributes ?? [])];
    assert(this.omit.every(x=>typeof x === 'string'), 'Invalid resource attribute exclusions');
    this.expectedResource=resourceKey(options.resource,this.omit);
    this.reducers=Object.assign(Object.create(null),options.gaugeReducers ?? {});
    assert(Object.values(this.reducers).every(x=>['sum','min','max','mean'].includes(x)), 'Invalid gauge reducer');
    assert(!('aggregationTemporality' in options), 'Only cumulative aggregation is supported');
    this.state=new ClusterState(this.maxSeries,this.maxWorkers);
    // Workers also report between scrapes: pushes, final snapshots and exits are applied as they arrive.
    cluster.on('message',this.onMessage); cluster.on('disconnect',this.onDisconnect); cluster.on('exit',this.onExit);
  }
  private diagnostic(message: string): void { try { this.options.onDiagnostic?.(message); } catch { /* Diagnostics must not change collection. */ } }
  /** Applied now, or after the round in flight so that round stays atomic. */
  private later(fn: ()=>void): void { if (this.pending) this.deferred.push(fn); else this.apply(fn); }
  private apply(fn: ()=>void): void { try { fn(); } catch (error) { this.diagnostic(reason(error)); } }
  private drain(): void { const queue=this.deferred; this.deferred=[]; for (const fn of queue) this.apply(fn); }
  private onMessage = (worker: Worker, raw: unknown): void => {
    if (this.stopped || !isOurMessage(raw)) return;
    const id=String(worker.id);
    let v: any;
    try { v=parseEnvelope(raw,this.maxBytes); } catch { this.diagnostic(`Discarded malformed message from worker ${id}`); return; }
    if (v.kind !== 'push' && v.kind !== 'final') return; // Replies belong to the round that requested them.
    this.later(()=>{
      try { this.state.accept(id,this.snapshot(id,v),v.kind === 'final'); }
      catch (error) { this.diagnostic(`Discarded ${v.kind} from worker ${id}: ${reason(error)}`); }
    });
  };
  private onDisconnect = (worker: Worker): void => { const id=String(worker.id); this.later(()=>this.state.gone(id)); };
  private onExit = (worker: Worker): void => { const id=String(worker.id); this.later(()=>this.state.forget(id)); };
  /** Validates one envelope against this cluster and turns it into the worker's snapshot. */
  private snapshot(id: string, v: any): WorkerState {
    assert(typeof v.bootId === 'string' && v.bootId.length > 0 && v.bootId.length <= 128 && Number.isSafeInteger(v.sequence) && v.sequence > 0 && Number.isSafeInteger(v.collectedAt) && v.collectedAt > 0, `Invalid ${v.kind} envelope`);
    assert(v.error === undefined, `Worker ${id}: ${String(v.error)}`);
    const metrics=decodeMetrics(v.metrics,this.maxSeries);
    assert(resourceKey(metrics.resource,this.omit) === this.expectedResource, `Resource conflict from worker ${id}`);
    for (const scope of metrics.scopeMetrics) for (const metric of scope.metrics) for (const point of metric.dataPoints) {
      assert(point.endTime[0]*1000 + point.endTime[1]/1e6 <= v.collectedAt+1, 'Point timestamp is after collection');
    }
    return {bootId:v.bootId,sequence:v.sequence,rows:rowsOf(metrics,`${id}:${v.bootId}`)};
  }
  collect(options: MetricCollectOptions = {}): Promise<CollectionResult> {
    if (this.stopped) return Promise.reject(new Error('ClusterMetricProducer is shutdown'));
    if (this.pending) return this.pending;
    let timeout = this.timeout;
    if (options.timeoutMillis !== undefined) { positive(options.timeoutMillis,'timeoutMillis'); timeout=Math.min(timeout,options.timeoutMillis); }
    this.pending=this.round(timeout).finally(()=>{this.pending=undefined; this.drain();}); return this.pending;
  }
  private async round(timeout: number): Promise<CollectionResult> {
    // A disconnected worker cannot answer and a finished one has already reported; both are retired on arrival of their event.
    const workers=Object.values(cluster.workers ?? {}).filter((w): w is Worker=>!!w && !w.isDead() && w.isConnected() && !this.state.isFinished(String(w.id)));
    assert(workers.length <= this.maxWorkers, 'Worker limit exceeded');
    const replies = await this.request(workers,timeout);
    assert(!this.stopped, 'ClusterMetricProducer is shutdown');
    return {resourceMetrics:this.state.round(this.options.resource,replies,this.reducers,this.start,hrTime()),errors:[]};
  }
  private request(workers: Worker[], timeout: number): Promise<Map<string,WorkerState>> {
    if (!workers.length) return Promise.resolve(new Map());
    const requestId=randomUUID(), requestedAt=Date.now();
    return new Promise((resolve,reject)=>{
      const replies=new Map<string,WorkerState>(), cleanup:(()=>void)[]=[];
      let done=false;
      const finish=(error?: Error):void=>{
        if (done) return; done=true; clearTimeout(timer); cleanup.forEach(f=>f()); this.cancelRound=undefined;
        if (error) reject(error); else resolve(replies);
      };
      const timer=setTimeout(()=>finish(new Error(`Missing workers: ${workers.filter(w=>!replies.has(String(w.id))).map(w=>w.id).join(', ')}`)),timeout);
      this.cancelRound=finish;
      // Attach every listener before sending any request.
      for (const w of workers) {
        const id=String(w.id);
        const listener=(raw: unknown):void=>{
          if (!isOurMessage(raw) || done) return;
          try {
            const v=parseEnvelope(raw,this.maxBytes);
            if (v.kind === 'push') return;
            if (v.kind === 'final') { if (replies.has(id)) return; } // A final is the reply of a worker on its way out.
            else {
              if (v.requestId !== requestId) { this.diagnostic(`Discarded late reply from worker ${id}`); return; }
              if (replies.has(id)) { this.diagnostic(`Discarded duplicate reply from worker ${id}`); return; }
              assert(v.kind === 'reply' && v.requestedAt === requestedAt && Number.isSafeInteger(v.collectedAt) && v.collectedAt >= requestedAt && v.collectedAt <= Date.now(), 'Invalid reply envelope');
            }
            replies.set(id,this.snapshot(id,v));
            if (replies.size === workers.length) finish();
          } catch (error) { finish(error instanceof Error ? error : new Error('Invalid worker response')); }
        };
        const left=(what: string)=>():void=>{ if (!replies.has(id)) finish(new Error(`Worker ${id} ${what} during collection`)); };
        const exit=left('exited'), disconnect=left('disconnected');
        w.on('message',listener); w.once('exit',exit); w.once('disconnect',disconnect);
        cleanup.push(()=>{w.off('message',listener);w.off('exit',exit);w.off('disconnect',disconnect);});
      }
      for (const w of workers) {
        if (done) break;
        if (!w.isConnected()) { finish(new Error(`Worker ${w.id} disconnected during collection`)); break; }
        // A send that fails (EPIPE, closed channel) means the worker left between the listing and the request.
        const unreachable=():void=>finish(new Error(`Worker ${w.id} disconnected during collection`));
        try { w.send(JSON.stringify({channel:CHANNEL,version:VERSION,kind:'collect',requestId,requestedAt,timeoutMillis:timeout}),error=>{if(error) unreachable();}); }
        catch { unreachable(); }
      }
    });
  }
  /** Size of committed transport state, for operational checks. */
  get stateSize(): {workers:number;series:number;retiredSeries:number} { return this.state.size; }
  async shutdown(): Promise<void> {
    this.stopped=true;
    cluster.off('message',this.onMessage); cluster.off('disconnect',this.onDisconnect); cluster.off('exit',this.onExit);
    this.cancelRound?.(new Error('ClusterMetricProducer is shutdown')); this.deferred=[]; this.state.clear();
  }
}

/** Use with exporter preventServerStart:true. The stock server returns 200 on collection failure. */
export function createPrometheusHandler(exporter: PrometheusExporter, serializer = new PrometheusSerializer()): (_request: IncomingMessage, response: ServerResponse)=>Promise<void> {
  return async (_request,response)=>{
    try {
      const result=await exporter.collect();
      assert(result.errors.length === 0,'Metrics collection failed');
      const body=serializer.serialize(result.resourceMetrics);
      response.writeHead(200,{'content-type':'text/plain; version=0.0.4; charset=utf-8'}); response.end(body);
    } catch { response.writeHead(503,{'content-type':'text/plain; charset=utf-8'}); response.end('Cluster metrics collection failed\n'); }
  };
}
