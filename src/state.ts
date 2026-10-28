import type { HrTime } from '@opentelemetry/api';
import type { ResourceMetrics } from '@opentelemetry/sdk-metrics';
import { assert } from './protocol.js';
import { merge, reconcile, retire, type GaugeReducer, type Row, type WorkerState } from './merge.js';

function entries(states: Map<string,WorkerState>, retired: Map<string,Row>): number {
  let n = retired.size; for (const w of states.values()) n += w.rows.size; return n;
}
/** Committed primary state: one latest snapshot per live worker and one retained total per exited series. */
export class ClusterState {
  private states = new Map<string,WorkerState>();
  private retired = new Map<string,Row>();
  private finished = new Set<string>();
  constructor(private readonly maxSeries: number, private readonly maxWorkers: number) {}
  /** One worker's latest snapshot outside a round: a push or a final. Throws and changes nothing when it does not reconcile. */
  accept(id: string, incoming: WorkerState, final = false): void {
    const old = this.states.get(id);
    // A snapshot the round already committed, such as a final that served as the reply, is known.
    if (!old || old.bootId !== incoming.bootId || old.sequence < incoming.sequence) {
      const retired = new Map(this.retired), states = new Map(this.states);
      states.set(id,reconcile(old,incoming,retired));
      assert(states.size <= this.maxWorkers, 'Worker limit exceeded');
      assert(entries(states,retired) <= this.maxSeries, 'Retained series limit exceeded');
      this.states = states; this.retired = retired;
    }
    if (final) { this.finished.add(id); this.gone(id); }
  }
  /** The worker will never report again: its last snapshot folds into the retained totals. */
  gone(id: string): void {
    const state = this.states.get(id);
    if (!state) return;
    const retired = new Map(this.retired);
    for (const row of state.rows.values()) retire(retired,row);
    this.retired = retired; this.states.delete(id);
  }
  /** The worker process exited. */
  forget(id: string): void { this.finished.delete(id); this.gone(id); }
  /** A worker that sent its final snapshot cannot answer a round any more. */
  isFinished(id: string): boolean { return this.finished.has(id); }
  /** A whole round commits together or not at all. Workers absent from the replies have left. */
  round(resource: ResourceMetrics['resource'], replies: Map<string,WorkerState>, reducers: Record<string,GaugeReducer>, start: HrTime, end: HrTime): ResourceMetrics {
    const states = new Map<string,WorkerState>(), retired = new Map(this.retired);
    for (const [id,old] of this.states) if (!replies.has(id)) for (const row of old.rows.values()) retire(retired,row);
    for (const [id,incoming] of replies) states.set(id,reconcile(this.states.get(id),incoming,retired));
    const result = merge(resource,states,retired,reducers,start,end,this.maxSeries);
    this.states = states; this.retired = retired;
    return result;
  }
  get size(): {workers:number;series:number;retiredSeries:number} {
    return {workers:this.states.size,series:entries(this.states,this.retired),retiredSeries:this.retired.size};
  }
  clear(): void { this.states.clear(); this.retired.clear(); this.finished.clear(); }
}
