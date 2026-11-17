#!/usr/bin/env node
import { checkCounting, validateCheckOptions, type CheckOptions } from './check.js';

const usage = `Usage: otel-cluster-metrics check --target <url> --metrics <url> --counter <name>
                                   [--duration <seconds>] [--rate <requests/second>]
                                   [--scrape <seconds>] [--timeout <seconds>]

Sends GET requests and compares received responses with a Prometheus request counter.
Use a quiet test or staging app. The counter must add one per request, including errors,
and exclude /metrics. Redirects are counted as responses, not followed.

  --target    URL to send GET requests to, e.g. http://localhost:3000/
  --metrics   Prometheus endpoint, e.g. http://localhost:9464/metrics
  --counter   counter name on /metrics, e.g. http_requests_total (or http_requests)
  --duration  seconds of traffic (default 60)
  --rate      requests a second (default 20)
  --scrape    seconds between reads, after the previous read finishes (default 5)
  --timeout   deadline per request or scrape in seconds (default 5)

Each request and scrape uses a fresh connection. These are reset-adjusted sample
changes, without PromQL extrapolation. A dashboard with different scrapes may differ.
Exits 0 within 2% with no request errors, 1 for mismatch or inconclusive traffic,
and 2 for bad input or an unreadable counter.`;

function parse(argv: string[]): CheckOptions {
  const allowed = new Set(['target', 'metrics', 'counter', 'duration', 'rate', 'scrape', 'timeout']);
  const args: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const match = /^--([^=]+)(?:=(.*))?$/.exec(argv[i]);
    if (!match || !allowed.has(match[1])) throw new Error(`Unknown option ${argv[i]}`);
    const key = match[1];
    const value = match[2] ?? argv[++i];
    if (!value || value.startsWith('--')) throw new Error(`Missing value for --${key}`);
    if (key in args) throw new Error(`Repeated option --${key}`);
    args[key] = value;
  }
  for (const key of ['target', 'metrics', 'counter']) if (!args[key]) throw new Error(`--${key} is required`);
  const numeric = (key: string, fallback: number): number => {
    if (!(key in args)) return fallback;
    if (!/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(args[key])) throw new Error(`--${key} must be a positive number`);
    return Number(args[key]);
  };
  const options: CheckOptions = {
    target: args.target, metrics: args.metrics, counter: args.counter,
    durationMillis: numeric('duration', 60) * 1000, rate: numeric('rate', 20),
    scrapeMillis: numeric('scrape', 5) * 1000, requestTimeoutMillis: numeric('timeout', 5) * 1000,
  };
  validateCheckOptions(options);
  return options;
}
const fmt = (n: number): string => n.toLocaleString('en-US', { maximumFractionDigits: 6 });

async function main(): Promise<number> {
  const [command, ...rest] = process.argv.slice(2);
  if (command === undefined || command === '--help' || command === '-h' || (command === 'check' && rest.length === 1 && ['--help', '-h'].includes(rest[0]))) {
    console.log(usage); return 0;
  }
  if (command !== 'check') { console.error(usage); return 2; }
  let options: CheckOptions;
  try { options = parse(rest); }
  catch (error) { console.error(error instanceof Error ? error.message : error); console.error(usage); return 2; }
  console.log(`Sending ${options.rate} GET requests a second to ${options.target} for ${options.durationMillis! / 1000}s.`);
  console.log(`Reading ${options.counter} from ${options.metrics} every ${options.scrapeMillis! / 1000}s on fresh connections.`);
  const result = await checkCounting(options);
  console.log(`Sent ${fmt(result.sent)} requests. Received ${fmt(result.answered)} complete responses (${fmt(result.httpErrors)} HTTP 4xx/5xx). Failed ${fmt(result.failed)} requests.`);
  console.log(`Sampled counter increase: ${fmt(result.counted)} from ${options.counter} (${fmt(result.scrapes)} scrapes, ${fmt(result.drops)} counter drops).`);
  if (result.ratio === null) {
    console.log('No ratio available: some responses were lost or none arrived. The counter may include requests whose responses never reached this check.');
    return 1;
  }
  console.log(`Counter / received responses: ${result.ratio.toFixed(2)}x what the app really answered.`);
  console.log('This compares sampled counter changes. A dashboard with different scrapes or PromQL may differ.');
  if (result.httpErrors) {
    console.log('The app returned HTTP errors. Fix those before using this as a healthy traffic check.');
    return 1;
  }
  if (Math.abs(result.ratio - 1) <= 0.02) { console.log('Your graph matches this check (within 2%).'); return 0; }
  console.log('Your graph does not match this check.');
  console.log('If workers share a metrics port, each scrape may read a different worker. @adityareddy-dev/otel-cluster-metrics merges their counters in the primary. Also check the counter and other traffic.');
  return 1;
}
main().then(code => { process.exitCode = code; }, error => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 2;
});
