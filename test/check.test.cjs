const {test} = require('node:test');
const assert = require('node:assert/strict');
const { fork, execFile } = require('node:child_process');
const { once } = require('node:events');
const { createServer } = require('node:net');
const path = require('node:path');
const { checkCounting, parseCounter, CounterTracker } = require('../dist');

async function freePort() {
  const s = createServer(); s.listen(0, '127.0.0.1'); await once(s, 'listening');
  const { port } = s.address(); await new Promise(r => s.close(r)); return port;
}
async function app(t, mode) {
  const appPort = await freePort(), metricsPort = await freePort();
  const child = fork(path.join(__dirname, 'check-app.cjs'), { env: { ...process.env, MODE: mode, APP_PORT: String(appPort), METRICS_PORT: String(metricsPort) } });
  t.after(async () => { if (child.exitCode === null) { const exited = once(child, 'exit'); child.kill('SIGTERM'); await exited; } });
  await new Promise((resolve, reject) => { child.on('message', m => { if (m === 'listening') resolve(); }); child.once('exit', reject); });
  return { target: `http://127.0.0.1:${appPort}/`, metrics: `http://127.0.0.1:${metricsPort}/metrics` };
}

test('parseCounter reads every series of one counter, with or without _total', () => {
  const text = '# TYPE requests_total counter\nrequests_total{route="/a"} 3\nrequests_total{route="/b"} 4 1700000000000\nother_total 9\n';
  assert.deepEqual([...parseCounter(text, 'requests').values()], [3, 4]);
  assert.deepEqual([...parseCounter(text, 'requests_total').values()], [3, 4]);
});

test('CounterTracker sums sample deltas and treats a drop as a reset', () => {
  const t = new CounterTracker();
  for (const v of [10, 15, 4, 9]) t.add(new Map([['x', v]]));
  assert.equal(t.counted, 5 + 4 + 5);
  assert.equal(t.drops, 1);
  t.add(new Map([['x', 9], ['y', 6]]));
  assert.equal(t.counted, 5 + 4 + 5 + 6, 'a series that shows up later started from 0');
});

test('a merged cluster counts what the app really answered', async t => {
  const urls = await app(t, 'merged');
  const result = await checkCounting({ ...urls, counter: 'requests', durationMillis: 3000, rate: 50, scrapeMillis: 300 });
  assert.ok(result.answered >= 140, `answered ${result.answered}`);
  assert.equal(result.failed, 0);
  assert.equal(result.counted, result.answered);
  assert.equal(result.drops, 0);
});

test('the stock exporter on a shared port does not count all the answers', async t => {
  const urls = await app(t, 'stock');
  const result = await checkCounting({ ...urls, counter: 'requests', durationMillis: 3000, rate: 50, scrapeMillis: 300 });
  assert.ok(result.ratio < 0.9 || result.ratio > 1.1, `ratio ${result.ratio}`);
});

test('the command exits 1 and explains when the graph does not match', async t => {
  const urls = await app(t, 'stock');
  const cli = path.join(__dirname, '..', 'dist', 'cli.js');
  const { code, stdout } = await new Promise(resolve => execFile(process.execPath, [cli, 'check', '--target', urls.target, '--metrics', urls.metrics, '--counter', 'requests', '--duration', '3', '--rate', '50', '--scrape', '0.3'], (error, stdout) => resolve({ code: error ? error.code : 0, stdout })));
  assert.equal(code, 1, stdout);
  assert.match(stdout, /what the app really answered/);
  assert.match(stdout, /does not match/);
});

test('the command exits 2 on missing input', async () => {
  const cli = path.join(__dirname, '..', 'dist', 'cli.js');
  const code = await new Promise(resolve => execFile(process.execPath, [cli, 'check', '--target', 'http://127.0.0.1:1/'], error => resolve(error ? error.code : 0)));
  assert.equal(code, 2);
});


async function endpoint(t, handler) {
  const server = require('node:http').createServer(handler);
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
  return `http://127.0.0.1:${server.address().port}`;
}
async function runCli(args) {
  return new Promise(resolve => execFile(process.execPath, [path.join(__dirname, '..', 'dist', 'cli.js'), ...args],
    (error, stdout, stderr) => resolve({ code: error ? error.code : 0, stdout, stderr })));
}
const quick = { durationMillis: 150, rate: 40, scrapeMillis: 40, requestTimeoutMillis: 100 };

test('a missing counter fails even if the app never answers', async t => {
  const url = await endpoint(t, (req, res) => res.end('other_total 1\n'));
  await assert.rejects(checkCounting({ ...quick, target: url, metrics: url, counter: 'missing' }), /missing.*not found/);
});

test('a measured flat counter reports zero instead of pretending it cannot measure', async t => {
  const url = await endpoint(t, (req, res) => res.end('requests_total 12\n'));
  const result = await checkCounting({ ...quick, target: url, metrics: url, counter: 'requests' });
  assert.ok(result.answered > 0);
  assert.equal(result.counted, 0);
  assert.equal(result.ratio, 0);
});

test('complete HTTP errors are received responses and are reported separately', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end(`requests_total ${count}\n`);
    count++; res.writeHead(503); res.end('unavailable');
  });
  const result = await checkCounting({ ...quick, target: url, metrics: `${url}/metrics`, counter: 'requests' });
  assert.equal(result.answered, count);
  assert.ok(count > 0);
  assert.equal(result.httpErrors, count);
  assert.equal(result.failed, 0);
  assert.equal(result.ratio, 1);
  const cli = await runCli(['check', '--target', url, '--metrics', `${url}/metrics`, '--counter', 'requests', '--duration', '0.15', '--scrape', '0.04']);
  assert.equal(cli.code, 1, JSON.stringify(cli));
  assert.match(cli.stdout, /HTTP 4xx\/5xx/);
  assert.doesNotMatch(cli.stdout, /Your graph matches/);
});

test('lost and truncated responses do not produce a ratio', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end(`requests_total ${count}\n`);
    count++;
    if (count % 3 === 0) { res.writeHead(200, { 'content-length': '100' }); res.write('partial'); setImmediate(() => res.destroy()); }
    else if (count % 3 === 1) req.socket.destroy();
    else res.end('ok');
  });
  const result = await checkCounting({ ...quick, target: url, metrics: `${url}/metrics`, counter: 'requests' });
  assert.ok(result.answered > 0);
  assert.ok(result.failed > 0);
  assert.equal(result.sent, result.answered + result.failed);
  assert.equal(result.ratio, null);
});

test('scrapes are serial and a final scrape covers all received responses', async t => {
  let count = 0, active = 0, maxActive = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url !== '/metrics') { count++; return res.end('ok'); }
    active++; maxActive = Math.max(maxActive, active);
    const snapshot = count;
    setTimeout(() => { active--; res.end(`requests_total ${snapshot}\n`); }, 60);
  });
  const result = await checkCounting({ ...quick, requestTimeoutMillis: 500, target: url, metrics: `${url}/metrics`, counter: 'requests' });
  assert.equal(maxActive, 1);
  assert.equal(result.counted, result.answered);
});

test('a failed final scrape fails without a plausible ratio', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url !== '/metrics') { count++; return res.end('ok'); }
    if (count) { res.writeHead(503); return res.end('failed'); }
    res.end('requests_total 0\n');
  });
  await assert.rejects(checkCounting({ ...quick, target: url, metrics: `${url}/metrics`, counter: 'requests' }), /Could not read.*HTTP 503/);
});

test('counter identity ignores label ordering and handles braces and escapes in values', () => {
  const tracker = new CounterTracker();
  tracker.add(parseCounter('requests_total{b="x}y",a="quote\\\""} 10\n', 'requests'));
  tracker.add(parseCounter('requests_total{a="quote\\\"",b="x}y"} 12\n', 'requests'));
  assert.equal(tracker.counted, 2);
});

test('invalid counter samples cannot silently become a plausible total', () => {
  for (const body of ['requests_total NaN\n', 'requests_total +Inf\n', 'requests_total -1\n',
    'requests_total 4\nrequests_total 5\n', '# TYPE requests_total gauge\nrequests_total 1\n', '#  TYPE  requests_total  gauge\nrequests_total 1\n',
    'requests_total{a="unterminated} 1\n', 'requests 1\nrequests_total 2\n']) {
    assert.throws(() => parseCounter(body, 'requests'), /counter|Counter/);
  }
});

test('bad options fail before any traffic', async t => {
  let hits = 0;
  const url = await endpoint(t, (req, res) => { hits++; res.end('requests_total 0\n'); });
  for (const invalid of [{durationMillis: Infinity}, {rate: NaN}, {requestTimeoutMillis: 0}, {scrapeMillis: Infinity},
    {target: 'file:///tmp/a'}, {metrics: 'ftp://localhost'}, {counter: ''}]) {
    await assert.rejects(checkCounting({ ...quick, target: url, metrics: url, counter: 'requests', ...invalid }));
  }
  assert.equal(hits, 0);
});

test('CLI rejects unknown options, malformed numbers and missing option values', async () => {
  for (const args of [ ['--target', '--metrics', 'http://localhost', '--counter', 'requests'],
    ['--target', 'http://localhost', '--metrics', 'http://localhost', '--counter', 'requests', '--typo', '1'],
    ['--target', 'http://localhost', '--metrics', 'http://localhost', '--counter', 'requests', '--duration', 'Infinity'] ]) {
    const result = await runCli(['check', ...args]);
    assert.equal(result.code, 2);
    assert.doesNotMatch(result.stdout, /Sending/);
  }
  const help = await runCli(['check', '--help']);
  assert.equal(help.code, 0);
  assert.match(help.stdout, /Usage:/);
});

test('an unreadable or malformed counter makes the CLI exit 2 without a ratio', async t => {
  for (const bad of ['# TYPE requests_total gauge\nrequests_total 2\n', 'requests_total NaN\n', 'unrelated_total 1\n']) {
    const url = await endpoint(t, (req, res) => res.end(bad));
    const result = await runCli(['check', '--target', url, '--metrics', url, '--counter', 'requests', '--duration', '0.1', '--scrape', '0.02']);
    assert.equal(result.code, 2, JSON.stringify(result));
    assert.match(result.stderr, /counter|not found/);
    assert.doesNotMatch(result.stdout, /Counter \/ received|Your graph matches|NaNx/);
  }
});

test('zero responses leaves the ratio unknown and exits 1', async t => {
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end('requests_total 0\n');
    req.socket.destroy();
  });
  const result = await runCli(['check', '--target', url, '--metrics', `${url}/metrics`, '--counter', 'requests', '--duration', '0.15', '--scrape', '0.02']);
  assert.equal(result.code, 1);
  assert.match(result.stdout, /Received 0 complete responses/);
  assert.match(result.stdout, /No ratio available/);
  assert.doesNotMatch(result.stdout, /NaN|Your graph matches/);
});

test('a request deadline covers a response that never ends', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end(`requests_total ${count}\n`);
    count++;
    res.write('partial');
    const timer = setInterval(() => res.write('.'), 5);
    res.once('close', () => clearInterval(timer));
  });
  const result = await checkCounting({ ...quick, target: url, metrics: `${url}/metrics`, counter: 'requests', requestTimeoutMillis: 30 });
  assert.ok(result.failed > 0);
  assert.equal(result.answered, 0);
  assert.equal(result.failed, result.sent);
  assert.equal(result.ratio, null);
});

test('an initially absent lazy counter starts at zero when it first appears', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end(count ? `requests_total ${count}\n` : '# no samples yet\n');
    count++; res.end('ok');
  });
  const result = await checkCounting({ ...quick, target: url, metrics: `${url}/metrics`, counter: 'requests' });
  assert.ok(result.answered > 0);
  assert.equal(result.counted, result.answered);
  assert.equal(result.ratio, 1);
});

test('a missing baseline endpoint fails before sending any traffic', async t => {
  let hits = 0;
  const target = await endpoint(t, (req, res) => { hits++; res.end('ok'); });
  const metrics = await endpoint(t, (req, res) => { res.writeHead(503); res.end('unavailable'); });
  await assert.rejects(checkCounting({ ...quick, target, metrics, counter: 'requests' }), /Could not read.*HTTP 503/);
  assert.equal(hits, 0);
});

test('counter read errors report the settled request counts', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') {
      if (count) { res.writeHead(503); return res.end('unavailable'); }
      return res.end('requests_total 0\n');
    }
    count++; setTimeout(() => res.end('ok'), 60);
  });
  await assert.rejects(checkCounting({ ...quick, requestTimeoutMillis: 500, target: url, metrics: `${url}/metrics`, counter: 'requests' }), error => {
    const counts = /Sent (\d+), received (\d+), failed (\d+)/.exec(error.message);
    assert.ok(counts, error.message);
    assert.ok(Number(counts[1]) > 0);
    assert.equal(Number(counts[1]), Number(counts[2]));
    assert.equal(Number(counts[3]), 0);
    return true;
  });
});

test('CLI reports a counter that adds four per response as 4.00x', async t => {
  let count = 0;
  const url = await endpoint(t, (req, res) => {
    if (req.url === '/metrics') return res.end(`requests_total ${count}\n`);
    count += 4; res.end('ok');
  });
  const result = await runCli(['check', '--target', url, '--metrics', `${url}/metrics`, '--counter', 'requests', '--duration', '0.2', '--scrape', '0.04']);
  assert.equal(result.code, 1, JSON.stringify(result));
  assert.match(result.stdout, /4\.00x/);
  assert.match(result.stdout, /does not match/);
});

test('Prometheus signed finite floats are valid nonnegative counter samples', () => {
  assert.deepEqual([...parseCounter('requests_total +3.0e0\n', 'requests').values()], [3]);
});


test('HTTPS protocol selection accepts upper-case schemes', async t => {
  const url = await endpoint(t, (req, res) => res.end('requests_total 0\n'));
  // TLS sent to this HTTP endpoint must reach it and fail the handshake, rather than
  // being rejected locally for passing an HTTPS URL to the HTTP client.
  await assert.rejects(checkCounting({ ...quick, target: url, metrics: url.replace('http:', 'HTTPS:'), counter: 'requests' }), /EPROTO/);
});
