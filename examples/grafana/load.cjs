// Sends the same traffic to both copies of the shop API. The rate follows a
// one-hour wave between about 5 and 25 requests a second.
const TARGETS = (process.env.TARGETS ?? 'http://localhost:8080').split(',');
const PATHS = ['/products', '/products', '/products', '/cart', '/cart', '/checkout'];

function tick() {
  const phase = (Date.now() / 3600000) * 2 * Math.PI;
  const rate = 15 + 10 * Math.sin(phase);
  const path = PATHS[Math.floor(Math.random() * PATHS.length)];
  for (const t of TARGETS) fetch(t + path).then((r) => r.arrayBuffer()).catch(() => {});
  setTimeout(tick, (1000 / rate) * (0.5 + Math.random()));
}
tick();
