import { diag, type HrTime } from '@opentelemetry/api';
import { DataPointType, type MetricData, type ResourceMetrics, type ScopeMetrics } from '@opentelemetry/sdk-metrics';
import { assert, attributeKey, canonical, timeKey } from './protocol.js';

export type GaugeReducer = 'sum' | 'min' | 'max' | 'mean';
export interface Row { scope: ScopeMetrics['scope']; metric: MetricData; point: any; worker?: string; }
export interface WorkerState { bootId: string; sequence: number; rows: Map<string, Row>; }
export const scopeKey = (r: Row): string => canonical([r.scope.name,r.scope.version ?? '',r.scope.schemaUrl ?? '']);
export const descriptorKey = (r: Row): string => canonical([scopeKey(r),r.metric.descriptor.name]);
export const seriesKey = (r: Row): string => canonical([descriptorKey(r),attributeKey(r.point.attributes)]);
export const retained = (r: Row): boolean => r.metric.dataPointType === DataPointType.HISTOGRAM || r.metric.dataPointType === DataPointType.SUM && r.metric.isMonotonic;
export function rowsOf(resourceMetrics: ResourceMetrics, worker: string): Map<string,Row> {
  const rows = new Map<string,Row>();
  for (const s of resourceMetrics.scopeMetrics) for (const m of s.metrics) for (const p of m.dataPoints) {
    const row = {scope:s.scope,metric:m,point:p,worker}; rows.set(seriesKey(row),row);
  }
  return rows;
}
function compatible(a: Row, b: Row): void {
  const x = a.metric, y = b.metric;
  assert(x.descriptor.unit === y.descriptor.unit && x.descriptor.valueType === y.descriptor.valueType && x.dataPointType === y.dataPointType && (x as any).isMonotonic === (y as any).isMonotonic && x.aggregationTemporality === y.aggregationTemporality, `Incompatible descriptor: ${x.descriptor.name}`);
  if (x.dataPointType === DataPointType.HISTOGRAM) assert(canonical(a.point.value.buckets.boundaries) === canonical(b.point.value.buckets.boundaries), `Mismatched bucket boundaries: ${x.descriptor.name}`);
}
export function add(a: Row, b: Row): Row {
  compatible(a,b);
  let value: any;
  if (a.metric.dataPointType !== DataPointType.HISTOGRAM) value = a.point.value + b.point.value;
  else {
    const x = a.point.value, y = b.point.value;
    value = {count:x.count+y.count,buckets:{boundaries:x.buckets.boundaries,counts:x.buckets.counts.map((n:number,i:number)=>n+y.buckets.counts[i])}};
    assert(Number.isSafeInteger(value.count), 'Histogram count overflow');
    if (x.sum !== undefined && y.sum !== undefined) value.sum = x.sum + y.sum;
    for (const k of ['min','max']) {
      if (x.count === 0) { if (y[k] !== undefined) value[k] = y[k]; }
      else if (y.count === 0) { if (x[k] !== undefined) value[k] = x[k]; }
      else if (x[k] !== undefined && y[k] !== undefined) value[k] = k === 'min' ? Math.min(x[k],y[k]) : Math.max(x[k],y[k]);
    }
  }
  if (retained(a)) {
    if (typeof value === 'number') assert(Number.isFinite(value), 'Monotonic total overflow');
    else for (const k of ['sum','min','max']) assert(value[k] === undefined || Number.isFinite(value[k]), 'Histogram statistic overflow');
  }
  return {...a,metric:{...a.metric,descriptor:{...a.metric.descriptor,description:[a.metric.descriptor.description,b.metric.descriptor.description].sort()[0]}},point:{...a.point,value}};
}
export function retire(totals: Map<string,Row>, row: Row): void {
  if (!retained(row)) return;
  const key = seriesKey(row), old = totals.get(key);
  totals.set(key, old ? add(old,row) : {...row,worker:undefined});
}
export function reconcile(old: WorkerState | undefined, incoming: WorkerState, retired: Map<string,Row>): WorkerState {
  if (!old) return incoming;
  if (old.bootId !== incoming.bootId) { for (const row of old.rows.values()) retire(retired,row); return incoming; }
  assert(incoming.sequence > old.sequence, 'Stale worker sequence');
  const rows = new Map(incoming.rows);
  for (const [key, prev] of old.rows) {
    const next = rows.get(key);
    if (!next) { if (retained(prev)) rows.set(key,prev); continue; }
    compatible(prev,next);
    assert(next.point.endTime[0] > prev.point.endTime[0] || next.point.endTime[0] === prev.point.endTime[0] && next.point.endTime[1] >= prev.point.endTime[1], 'Point timestamp moved backwards');
    if (retained(prev)) {
      if (timeKey(prev.point.startTime) !== timeKey(next.point.startTime)) retire(retired,prev);
      else if (prev.metric.dataPointType === DataPointType.SUM) assert(next.point.value >= prev.point.value, 'Monotonic total decreased without a reset');
      else {
        assert(next.point.value.count >= prev.point.value.count && prev.point.value.buckets.counts.every((n:number,i:number)=>next.point.value.buckets.counts[i] >= n), 'Histogram decreased without a reset');
      }
    }
  }
  return {...incoming,rows};
}
export function merge(resource: ResourceMetrics['resource'], workers: Map<string,WorkerState>, retired: Map<string,Row>, reducers: Record<string,GaugeReducer>, start: HrTime, end: HrTime, maxSeries: number): ResourceMetrics {
  const all = [...retired.values(),...Array.from(workers.values()).flatMap(w=>Array.from(w.rows.values()))];
  assert(all.length <= maxSeries, 'Retained series limit exceeded');
  const descriptors = new Map<string,Row>(), output = new Map<string,Row>(), counts = new Map<string,number>();
  for (const row of all) {
    const dk = descriptorKey(row), descriptor = descriptors.get(dk);
    if (descriptor) {
      compatible(descriptor,row);
      if (descriptor.metric.descriptor.description !== row.metric.descriptor.description) diag.warn(`otel-cluster-metrics: description differs for ${row.metric.descriptor.name}`);
      if (row.metric.descriptor.description < descriptor.metric.descriptor.description) descriptors.set(dk,row);
    } else descriptors.set(dk,row);
    let key = seriesKey(row), current = row;
    if (row.metric.dataPointType === DataPointType.GAUGE) {
      const reducer = reducers[row.metric.descriptor.name];
      if (!reducer) {
        assert(row.point.attributes['cluster.worker.id'] === undefined, 'Reserved attribute: cluster.worker.id');
        current = {...row,point:{...row.point,attributes:{...row.point.attributes,'cluster.worker.id':row.worker}}}; key = seriesKey(current);
      }
      const existing = output.get(key);
      if (existing && reducer) {
        const a = existing.point.value, b = row.point.value;
        current = {...row,point:{...row.point,value:reducer === 'min' ? Math.min(a,b) : reducer === 'max' ? Math.max(a,b) : a+b}};
      }
      counts.set(key,(counts.get(key) ?? 0)+1);
    } else { const existing = output.get(key); if (existing) current = add(existing,row); }
    output.set(key,current);
  }
  const scopes = new Map<string,ScopeMetrics>(), metrics = new Map<string,MetricData>();
  for (const [key,row] of Array.from(output).sort(([a],[b])=>a.localeCompare(b))) {
    const sk = scopeKey(row), dk = descriptorKey(row);
    let scope = scopes.get(sk); if (!scope) { scope = {scope:row.scope,metrics:[]}; scopes.set(sk,scope); }
    let metric = metrics.get(dk);
    if (!metric) { metric = {...row.metric,descriptor:{...descriptors.get(dk)!.metric.descriptor},dataPoints:[]} as MetricData; metrics.set(dk,metric); scope.metrics.push(metric); }
    const value = row.metric.dataPointType === DataPointType.GAUGE && reducers[row.metric.descriptor.name] === 'mean' ? row.point.value / counts.get(key)! : row.point.value;
    (metric.dataPoints as any[]).push({...row.point,value,startTime:start,endTime:end});
  }
  return {resource,scopeMetrics:Array.from(scopes.values())};
}
