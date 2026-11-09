const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createServer } = require('node:http');
const { once } = require('node:events');
const { createTrafficSource } = require('../examples/grafana/load.cjs');

test('the traffic reference counts received 200 and 500 responses separately from connection failures', async (t) => {
  assert.equal(typeof createTrafficSource, 'function');
  const server = createServer((req, res) => {
    res.writeHead(req.url === '/checkout' ? 500 : 200);
    res.end('ok');
  }).listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => server.close(resolve)));
  const source = createTrafficSource([
    { name: 'shop-api', url: `http://127.0.0.1:${server.address().port}` },
    { name: 'unreachable', url: 'http://127.0.0.1:0' },
  ]);
  await source.send('/products');
  await source.send('/checkout');
  const metrics = source.metrics();
  assert.match(metrics, /load_requests_total\{target="shop-api"\} 2\n/);
  assert.match(metrics, /load_responses_total\{target="shop-api"\} 2\n/);
  assert.match(metrics, /load_errors_total\{target="shop-api"\} 0\n/);
  assert.match(metrics, /load_responses_total\{target="unreachable"\} 0\n/);
  assert.match(metrics, /load_errors_total\{target="unreachable"\} 2\n/);
  assert.match(metrics, /load_in_flight\{target="shop-api"\} 0\n/);
});

test('the traffic reference exposes requests still waiting for a response', async (t) => {
  assert.equal(typeof createTrafficSource, 'function');
  let answer;
  const arrived = new Promise(resolve => { answer = resolve; });
  const server = createServer((req, res) => answer(res)).listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => server.close(resolve)));
  const source = createTrafficSource([{ name: 'shop-api', url: `http://127.0.0.1:${server.address().port}` }]);
  const pending = source.send('/products');
  const res = await arrived;
  assert.match(source.metrics(), /load_in_flight\{target="shop-api"\} 1\n/);
  assert.match(source.metrics(), /load_responses_total\{target="shop-api"\} 0\n/);
  res.end('ok');
  await pending;
  assert.match(source.metrics(), /load_in_flight\{target="shop-api"\} 0\n/);
  assert.match(source.metrics(), /load_responses_total\{target="shop-api"\} 1\n/);
});
