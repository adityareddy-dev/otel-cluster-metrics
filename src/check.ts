import http from 'node:http';
import https from 'node:https';
import { setTimeout as delay } from 'node:timers/promises';

export interface CheckOptions {
  /** URL to send GET requests to. Redirects are not followed. */
  target: string;
  /** Prometheus text endpoint of the same app. */
  metrics: string;
  /** Counter that adds one per request, as exposed on /metrics, with or without _total. */
  counter: string;
  durationMillis?: number;
  /** Requests a second. */
  rate?: number;
  scrapeMillis?: number;
  requestTimeoutMillis?: number;
}
export interface CheckResult {
  sent: number;
  /** Complete responses, including HTTP errors. */
  answered: number;
  /** Requests with no complete response. */
  failed: number;
  /** Complete responses with status 400 or higher, included in answered. */
  httpErrors: number;
  counted: number;
  drops: number;
  scrapes: number;
  /** Reset-adjusted sample increase / answered. Unknown if any response was lost or none arrived. */
  ratio: number | null;
}

const metricName = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;

/** Read one counter, keeping label order out of series identity. */
export function parseCounter(text: string, name: string): Map<string, number> {
  if (!metricName.test(name)) throw new Error('Invalid counter name');
  const names = name.endsWith('_total') ? [name] : [name, `${name}_total`];
  const series = new Map<string, number>();
  let foundName: string | undefined;
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    const type = /^#\s+TYPE\s+(\S+)\s+(\S+)$/.exec(line);
    if (type && (names.includes(type[1]) || `${type[1]}_total` === name) && type[2] !== 'counter') {
      throw new Error(`${name} is not a counter (${type[2]})`);
    }
    if (!line || line.startsWith('#')) continue;
    const prefix = /^([a-zA-Z_:][a-zA-Z0-9_:]*)/.exec(line);
    if (!prefix || !names.includes(prefix[1])) continue;
    const match = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{((?:[^"{}]|"(?:\\.|[^"\\])*")*)\})?\s+(\S+)(?:\s+-?\d+)?(?:\s+# .*)?$/.exec(line);
    if (!match) throw new Error(`Invalid counter sample for ${name}`);
    if (foundName && foundName !== match[1]) throw new Error(`Ambiguous counter name ${name}: use the exact name on /metrics`);
    foundName = match[1];
    const labels: [string, string][] = [];
    let rest = match[2]?.trim() ?? '';
    while (rest) {
      const label = /^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\[\\"n]|[^"\\])*)"\s*(,\s*|$)/.exec(rest);
      if (!label || labels.some(([key]) => key === label[1])) throw new Error(`Invalid counter labels for ${name}`);
      labels.push([label[1], label[2]]);
      rest = rest.slice(label[0].length);
    }
    const value = Number(match[3]);
    if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(match[3]) || !Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Invalid counter value for ${name}`);
    }
    const key = `${match[1]}${JSON.stringify(labels.sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0))}`;
    if (series.has(key)) throw new Error(`Duplicate counter series for ${name}`);
    series.set(key, value);
  }
  return series;
}

/** Sum observed deltas, treating a decrease as a reset. No PromQL extrapolation. */
export class CounterTracker {
  private last = new Map<string, number>();
  private started = false;
  counted = 0;
  drops = 0;
  add(sample: Map<string, number>): void {
    for (const [key, value] of sample) {
      const before = this.last.get(key) ?? (this.started ? 0 : undefined);
      if (before !== undefined) {
        if (value >= before) this.counted += value - before;
        else { this.counted += value; this.drops += 1; }
      }
      this.last.set(key, value);
    }
    this.started = true;
  }
}

function get(url: string, timeout: number, readBody = false): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const client = new URL(url).protocol === 'https:' ? https : http;
    // Fresh connections let a shared cluster port choose a worker for each request and scrape.
    const request = client.get(url, { agent: false }, response => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', chunk => { if (readBody) body += chunk; });
      response.on('end', () => { clearTimeout(timer); resolve({ status: response.statusCode ?? 0, body }); });
      response.on('error', fail);
      response.on('aborted', () => fail(new Error('Response closed before it completed')));
    });
    const timer = setTimeout(() => request.destroy(new Error(`Request timed out after ${timeout}ms`)), timeout);
    function fail(error: Error): void { clearTimeout(timer); reject(error); }
    request.on('error', fail);
  });
}

/** Validate before opening either endpoint. Also used by the CLI before announcing traffic. */
export function validateCheckOptions(options: CheckOptions): void {
  for (const [name, url] of [['target', options.target], ['metrics', options.metrics]]) {
    let parsed: URL;
    try { parsed = new URL(url); } catch { throw new Error(`Invalid ${name} URL: use http:// or https://`); }
    if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error(`Invalid ${name} URL: use http:// or https://`);
  }
  if (!metricName.test(options.counter)) throw new Error('Invalid counter name: use the name exposed on /metrics');
  for (const [name, value] of [
    ['durationMillis', options.durationMillis ?? 60000], ['scrapeMillis', options.scrapeMillis ?? 5000],
    ['requestTimeoutMillis', options.requestTimeoutMillis ?? 5000],
  ] as const) {
    if (!Number.isFinite(value) || value < 1 || value > 2147483647) throw new Error(`${name} must be between 1 and 2147483647 milliseconds`);
  }
  const rate = options.rate ?? 20;
  if (!Number.isFinite(rate) || rate <= 0 || !Number.isSafeInteger(Math.ceil(rate * (options.durationMillis ?? 60000) / 1000))) {
    throw new Error('rate must be positive and schedule a finite, safe number of requests');
  }
}

/** Compare received responses with reset-adjusted counter changes from this run's scrapes. */
export async function checkCounting(options: CheckOptions): Promise<CheckResult> {
  validateCheckOptions(options);
  const duration = options.durationMillis ?? 60000;
  const rate = options.rate ?? 20;
  const scrapeEvery = options.scrapeMillis ?? 5000;
  const timeout = options.requestTimeoutMillis ?? 5000;
  const tracker = new CounterTracker();
  let sent = 0, answered = 0, failed = 0, httpErrors = 0, scrapes = 0;
  let lastSample = new Map<string, number>();
  const inFlight = new Set<Promise<void>>();
  const scrape = async (): Promise<void> => {
    try {
      const { status, body } = await get(options.metrics, timeout, true);
      if (status !== 200) throw new Error(`HTTP ${status}`);
      lastSample = parseCounter(body, options.counter);
      tracker.add(lastSample);
      scrapes++;
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      throw new Error(`Could not read ${options.counter} from ${options.metrics}: ${reason}`);
    }
  };
  try {
    await scrape();
    const send = (): void => {
      sent++;
      const request = get(options.target, timeout).then(({ status }) => {
        answered++;
        if (status >= 400) httpErrors++;
      }, () => { failed++; });
      inFlight.add(request);
      void request.then(() => inFlight.delete(request));
    };
    const started = performance.now();
    const loadTimer = setInterval(() => {
      const elapsed = performance.now() - started;
      // Never send a catch-up burst after the traffic window has closed.
      if (elapsed >= duration) return;
      const due = Math.floor(elapsed * rate / 1000);
      while (sent < due) send();
    }, Math.max(1, Math.min(50, 1000 / rate)));
    try {
      // Await each scrape before scheduling another, even if an endpoint is slow.
      while (performance.now() - started < duration) {
        await delay(Math.min(scrapeEvery, Math.max(1, duration - (performance.now() - started))));
        if (performance.now() - started < duration) await scrape();
      }
    } finally {
      clearInterval(loadTimer);
      await Promise.all(inFlight);
    }
    // Allow one interval for instrumentation to record the last completed request.
    await delay(scrapeEvery);
    await scrape();
    if (!lastSample.size) throw new Error(`${options.counter} not found on ${options.metrics}. Use the exact counter name exposed on /metrics`);
    return { sent, answered, failed, httpErrors, counted: tracker.counted, drops: tracker.drops, scrapes,
      ratio: answered > 0 && failed === 0 ? tracker.counted / answered : null };
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`${reason}. Sent ${sent}, received ${answered}, failed ${failed}. No ratio available.`);
  }
}
