import type { Attributes, HrTime } from '@opentelemetry/api';
import { AggregationTemporality, DataPointType, type MetricData, type ResourceMetrics } from '@opentelemetry/sdk-metrics';
import { resourceFromAttributes } from '@opentelemetry/resources';

export const CHANNEL = 'otel-cluster-metrics';
export const VERSION = 1;
export const DEFAULT_MAX_BYTES = 1024 * 1024;
export function assert(ok: unknown, message: string): asserts ok { if (!ok) throw new Error(message); }
export function positive(value: number, name: string): number {
  assert(Number.isSafeInteger(value) && value > 0, `Invalid ${name}`); return value;
}
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).filter(k => (value as any)[k] !== undefined).sort().map(k => JSON.stringify(k) + ':' + canonical((value as any)[k])).join(',') + '}';
  return JSON.stringify(value);
}
function encodeNumber(n: number): unknown {
  return Number.isFinite(n) ? n : { number: Number.isNaN(n) ? 'NaN' : n > 0 ? '+Infinity' : '-Infinity' };
}
function number(v: any): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (v && Object.keys(v).length === 1 && ['NaN', '+Infinity', '-Infinity'].includes(v.number)) {
    return v.number === 'NaN' ? NaN : v.number === '+Infinity' ? Infinity : -Infinity;
  }
  throw new Error('Invalid numeric field');
}
function attributes(a: any, encode = false): Attributes {
  assert(a && typeof a === 'object' && !Array.isArray(a), 'Invalid attributes');
  const out: any = Object.create(null);
  const scalar = (v: any): any => {
    if (typeof v === 'string' || typeof v === 'boolean') return v;
    if (encode) { assert(typeof v === 'number', 'Invalid numeric attribute'); return encodeNumber(v); }
    return number(v);
  };
  for (const key of Object.keys(a).sort()) {
    if (encode && a[key] === undefined) continue;
    const v = a[key];
    if (Array.isArray(v)) {
      out[key] = v.map(x => x === null ? null : scalar(x));
      const kinds = new Set(v.filter(x => x !== null).map(x => typeof (encode ? x : scalar(x))));
      assert(kinds.size <= 1, 'Mixed attribute array');
    } else out[key] = scalar(v);
  }
  return out;
}
export function attributeKey(a: Attributes): string { return canonical(attributes(a, true)); }
function time(v: any): HrTime {
  assert(Array.isArray(v) && v.length === 2 && Number.isSafeInteger(v[0]) && v[0] >= 0 && Number.isSafeInteger(v[1]) && v[1] >= 0 && v[1] < 1e9, 'Invalid timestamp');
  return [v[0], v[1]];
}
export function timeKey(t: HrTime): string { return `${t[0]}:${t[1]}`; }
export function resourceKey(r: ResourceMetrics['resource'], omit: string[] = []): string {
  const a = { ...r.attributes }; for (const k of omit) delete a[k];
  return canonical([attributes(a, true), r.schemaUrl ?? '']);
}
export function encodeMetrics(r: ResourceMetrics): unknown {
  assert(!r.resource.asyncAttributesPending, 'Resource attributes must be resolved');
  return {
    resource: { attributes: attributes(r.resource.attributes, true), schemaUrl: r.resource.schemaUrl },
    scopeMetrics: r.scopeMetrics.map(s => ({ scope: {name:s.scope.name, version:s.scope.version, schemaUrl:s.scope.schemaUrl}, metrics:s.metrics.map(m => ({
      descriptor: { ...m.descriptor }, aggregationTemporality:m.aggregationTemporality, dataPointType:m.dataPointType,
      ...(m.dataPointType === DataPointType.SUM ? {isMonotonic:m.isMonotonic} : {}),
      dataPoints:m.dataPoints.map(p => ({ attributes:attributes(p.attributes, true), startTime:p.startTime, endTime:p.endTime,
        value: typeof p.value === 'number' ? encodeNumber(p.value) : m.dataPointType === DataPointType.HISTOGRAM ? {
          ...p.value, sum:p.value.sum === undefined ? undefined : encodeNumber(p.value.sum),
          min:p.value.min === undefined ? undefined : encodeNumber(p.value.min), max:p.value.max === undefined ? undefined : encodeNumber(p.value.max)
        } : p.value
      }))
    })) }))
  };
}
export function decodeMetrics(v: any, maxPoints: number): ResourceMetrics {
  assert(v?.resource && Array.isArray(v.scopeMetrics), 'Invalid resource metrics');
  const r = v.resource;
  assert(r.schemaUrl === undefined || typeof r.schemaUrl === 'string', 'Invalid resource schema');
  let points = 0, descriptors = 0;
  const scopeKeys = new Set<string>();
  const scopeMetrics = v.scopeMetrics.map((s: any) => {
    assert(s?.scope && typeof s.scope.name === 'string' && Array.isArray(s.metrics), 'Invalid scope');
    for (const k of ['version', 'schemaUrl']) assert(s.scope[k] === undefined || typeof s.scope[k] === 'string', 'Invalid scope metadata');
    const scope = {name:s.scope.name, version:s.scope.version, schemaUrl:s.scope.schemaUrl};
    const sk = canonical(scope); assert(!scopeKeys.has(sk), 'Duplicate scope'); scopeKeys.add(sk);
    const names = new Set<string>();
    const metrics = s.metrics.map((m: any): MetricData => {
      assert(++descriptors <= maxPoints, 'Descriptor limit exceeded');
      const d = m?.descriptor;
      assert(d && typeof d.name === 'string' && /^[a-zA-Z][a-zA-Z0-9_.\-/]{0,254}$/.test(d.name) && typeof d.description === 'string' && typeof d.unit === 'string' && [0,1].includes(d.valueType), 'Invalid descriptor');
      assert(!names.has(d.name), 'Duplicate metric'); names.add(d.name);
      assert(m.aggregationTemporality === AggregationTemporality.CUMULATIVE, 'Only cumulative metrics are supported');
      assert([DataPointType.SUM,DataPointType.GAUGE,DataPointType.HISTOGRAM].includes(m.dataPointType), 'Unsupported data point type');
      assert(m.dataPointType !== DataPointType.SUM || typeof m.isMonotonic === 'boolean', 'Invalid monotonicity');
      assert(Array.isArray(m.dataPoints), 'Invalid data points');
      const attrs = new Set<string>();
      const dataPoints = m.dataPoints.map((p: any) => {
        assert(++points <= maxPoints, 'Point limit exceeded');
        const a = attributes(p.attributes); const key = attributeKey(a);
        assert(!attrs.has(key), 'Duplicate series'); attrs.add(key);
        const startTime = time(p.startTime), endTime = time(p.endTime);
        assert(startTime[0] < endTime[0] || startTime[0] === endTime[0] && startTime[1] <= endTime[1], 'Reversed point timestamps');
        let value: any;
        if (m.dataPointType !== DataPointType.HISTOGRAM) {
          value = number(p.value);
          assert(!(m.dataPointType === DataPointType.SUM && m.isMonotonic) || Number.isFinite(value) && value >= 0, 'Invalid monotonic total');
        } else {
          const h = p.value;
          assert(h && Number.isSafeInteger(h.count) && h.count >= 0 && Array.isArray(h.buckets?.boundaries) && Array.isArray(h.buckets?.counts), 'Invalid histogram');
          const b = h.buckets.boundaries, counts = h.buckets.counts;
          assert(counts.length === b.length + 1 && b.every((x: any,i: number) => typeof x === 'number' && Number.isFinite(x) && (i === 0 || x > b[i-1])) && counts.every((x: any) => Number.isSafeInteger(x) && x >= 0) && counts.reduce((a:number,b:number)=>a+b,0) === h.count, 'Invalid histogram buckets');
          value = { count:h.count, buckets:{boundaries:[...b],counts:[...counts]} };
          for (const k of ['sum','min','max']) if (h[k] !== undefined) { value[k] = number(h[k]); assert(Number.isFinite(value[k]), 'Non-finite histogram statistic'); }
          assert(value.min === undefined || value.max === undefined || value.min <= value.max, 'Invalid histogram extrema');
        }
        return {attributes:a,startTime,endTime,value};
      });
      return {descriptor:{name:d.name,description:d.description,unit:d.unit,valueType:d.valueType}, aggregationTemporality:m.aggregationTemporality, dataPointType:m.dataPointType, ...(m.dataPointType === DataPointType.SUM ? {isMonotonic:m.isMonotonic}:{}),dataPoints} as MetricData;
    });
    return {scope,metrics};
  });
  return {resource:resourceFromAttributes(attributes(r.attributes), {schemaUrl:r.schemaUrl}),scopeMetrics};
}
export function parseEnvelope(raw: unknown, maxBytes: number): any {
  assert(typeof raw === 'string' && Buffer.byteLength(raw) <= maxBytes, 'IPC envelope exceeds byte limit or is not a string');
  const v = JSON.parse(raw);
  assert(v?.channel === CHANNEL && v.version === VERSION, 'Unsupported IPC envelope'); return v;
}
export function isOurMessage(raw: unknown): boolean {
  return typeof raw === 'string' && raw.startsWith('{"channel":"' + CHANNEL + '"');
}
