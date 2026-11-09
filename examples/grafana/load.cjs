const { createServer } = require('node:http');

// Independent of either shop's SDK. A received 500 counts as a response too.
function createTrafficSource(targets) {
  const counts = targets.map(target => ({ ...target, requests: 0, responses: 0, errors: 0, inFlight: 0 }));
  const label = value => JSON.stringify(value).replace(/\\r/g, '\\n');
  return {
    async send(path) {
      await Promise.all(counts.map(async target => {
        target.requests++;
        target.inFlight++;
        try {
          const response = await fetch(target.url + path, { signal: AbortSignal.timeout(10000) });
          await response.arrayBuffer();
          target.responses++;
        } catch {
          target.errors++;
        } finally {
          target.inFlight--;
        }
      }));
    },
    metrics() {
      let text = '';
      for (const [name, field, type] of [
        ['load_requests_total', 'requests', 'counter'],
        ['load_responses_total', 'responses', 'counter'],
        ['load_errors_total', 'errors', 'counter'],
        ['load_in_flight', 'inFlight', 'gauge'],
      ]) {
        text += `# TYPE ${name} ${type}\n`;
        for (const target of counts) text += `${name}{target=${label(target.name)}} ${target[field]}\n`;
      }
      return text;
    },
  };
}

if (require.main === module) {
  const targets = (process.env.TARGETS ?? 'http://localhost:8080').split(',')
    .map(url => ({ url, name: new URL(url).hostname }));
  const source = createTrafficSource(targets);
  const paths = ['/products', '/products', '/products', '/cart', '/cart', '/checkout'];
  const server = createServer((req, res) => {
    if (req.url !== '/metrics') { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4; charset=utf-8' });
    res.end(source.metrics());
  }).listen(Number(process.env.METRICS_PORT ?? 9100), '0.0.0.0');
  let timer;
  function tick() {
    const phase = (Date.now() / 3600000) * 2 * Math.PI;
    const rate = 15 + 10 * Math.sin(phase);
    void source.send(paths[Math.floor(Math.random() * paths.length)]);
    timer = setTimeout(tick, (1000 / rate) * (0.5 + Math.random()));
  }
  tick();
  process.on('SIGTERM', () => { clearTimeout(timer); server.close(); });
}

module.exports = { createTrafficSource };
