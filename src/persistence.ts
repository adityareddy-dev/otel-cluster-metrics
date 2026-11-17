import { closeSync, fsyncSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { HrTime } from '@opentelemetry/api';
import type { ResourceMetrics } from '@opentelemetry/sdk-metrics';
import { assert, decodeMetrics, encodeMetrics, resourceKey, timeKey } from './protocol.js';

const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException).code === 'ENOENT';
const reason = (error: unknown): string => error instanceof Error ? error.message : String(error);
function read(file: string): string | undefined {
  try { return readFileSync(file,'utf8'); } catch (error) { if (missing(error)) return undefined; throw error; }
}

/** Versioned checkpoints. The sidecar lock covers comparison through atomic replacement. */
export class StateFile {
  private readonly file: string;
  private contents?: string;
  private conflicted = false;
  constructor(file: string, private readonly identity: string, private readonly diagnostic: (message: string)=>void) {
    assert(typeof file === 'string' && file.trim().length > 0 && !file.includes('\0'), 'Invalid stateFile');
    this.file = resolve(file);
  }
  load(maxSeries: number, resource: ResourceMetrics['resource']): {start: HrTime; metrics: ResourceMetrics} | undefined {
    try {
      this.contents = read(this.file);
      if (this.contents === undefined) return undefined;
      const saved = JSON.parse(this.contents);
      assert(saved?.version === 1, 'Unsupported state file version');
      assert(saved.identity === this.identity, 'State file configuration does not match');
      const t = saved.startTime;
      assert(Array.isArray(t) && t.length === 2 && Number.isSafeInteger(t[0]) && t[0] >= 0 && Number.isSafeInteger(t[1]) && t[1] >= 0 && t[1] < 1e9 && t[0]*1000+t[1]/1e6 <= Date.now()+1, 'Invalid state file start timestamp');
      const start: HrTime = [t[0],t[1]];
      const metrics = decodeMetrics(saved.metrics,maxSeries);
      assert(resourceKey(metrics.resource) === resourceKey(resource), 'State file resource does not match');
      for (const scope of metrics.scopeMetrics) for (const metric of scope.metrics) for (const point of metric.dataPoints) {
        assert(timeKey(point.startTime) === timeKey(start) && point.endTime[0]*1000+point.endTime[1]/1e6 <= Date.now()+1, 'Invalid state file point timestamps');
      }
      return {start,metrics};
    } catch (error) {
      const message = `Cannot restore state file ${this.file}: ${reason(error)}`;
      this.diagnostic(message); throw new Error(message);
    }
  }
  save(start: HrTime, metrics: ResourceMetrics): void {
    if (this.conflicted) return;
    const lock = this.file+'.lock', temp = `${this.file}.${process.pid}.${randomUUID()}.tmp`;
    let lockFd: number | undefined, tempFd: number | undefined, tempOwned = false;
    try {
      const body = JSON.stringify({version:1,identity:this.identity,startTime:start,metrics:encodeMetrics(metrics)});
      lockFd = openSync(lock,'wx',0o600);
      writeFileSync(lockFd,JSON.stringify({pid:process.pid}));
      if (read(this.file) !== this.contents) {
        this.conflicted = true;
        throw new Error('State file changed by another producer; further saves disabled');
      }
      tempFd = openSync(temp,'wx',0o600); tempOwned = true;
      writeFileSync(tempFd,body); fsyncSync(tempFd); closeSync(tempFd); tempFd = undefined;
      renameSync(temp,this.file); tempOwned = false; this.contents = body;
    } catch (error) { this.diagnostic(`Cannot persist state file ${this.file}: ${reason(error)}`); }
    finally {
      // Only remove files acquired by this writer. A busy or stale lock belongs to someone else.
      if (tempFd !== undefined) { try { closeSync(tempFd); } catch {} }
      if (lockFd !== undefined) {
        try { if (tempOwned) unlinkSync(temp); } catch (error) { if (!missing(error)) this.diagnostic(`Cannot remove state temporary file: ${reason(error)}`); }
        try { closeSync(lockFd); } catch {}
        try { unlinkSync(lock); } catch (error) { this.diagnostic(`Cannot remove state lock: ${reason(error)}`); }
      }
    }
  }
}
