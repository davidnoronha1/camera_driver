// End-to-end browser test: serves web/, opens it in headless Chromium with a
// fake camera, runs the default pipeline and checks that frames reach the canvas
// display and the JPEG-encoded branch.
//   zig build wasm && NODE_PATH=$(npm root -g) node web/test_browser.mjs
import { createServer } from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { chromium } = require('playwright');

const root = fileURLToPath(new URL('.', import.meta.url));
const types = { '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm' };
const server = createServer((req, res) => {
  const p = join(root, req.url === '/' ? 'index.html' : req.url.split('?')[0]);
  if (!existsSync(p)) { res.writeHead(404).end(); return; }
  res.writeHead(200, { 'content-type': types[extname(p)] ?? 'application/octet-stream' }).end(readFileSync(p));
}).listen(0);
const port = server.address().port;

const browser = await chromium.launch({
  args: ['--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream', '--enable-unsafe-webgpu', '--use-angle=swiftshader'],
});
const page = await browser.newPage();
const errors = [];
page.on('pageerror', (e) => errors.push(String(e)));
page.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
await page.goto(`http://localhost:${port}/`);
await page.waitForFunction(() => window.__ready === true, null, { timeout: 15000 });
console.log('caps:', await page.textContent('#caps'));

await page.click('#run');
await page.waitForFunction(() => (window.__sinkFrames ?? 0) >= 5, null, { timeout: 15000 });
const result = await page.evaluate(() => {
  const c = document.getElementById('view');
  const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
  let nonzero = 0;
  for (let i = 0; i < d.length; i += 4) if (d[i] | d[i + 1] | d[i + 2]) nonzero++;
  return { w: c.width, h: c.height, nonzero, sink: window.__sinkFrames, stats: document.getElementById('stats').textContent };
});
console.log(result);
await page.click('#stop');

// Scenario 2: push-source -> undistort -> callback-sink, on both undistort
// implementations (WebGPU when the browser offers it, wasm CPU always).
const undistort = await page.evaluate(async () => {
  const { Core, Runtime, detectWebCaps } = await import('./host.js');
  const core = await Core.load('./camera_driver.wasm');
  const yaml = `elements:
  - type: CustomSrcElement
    format: RGB
    width: 64
    height: 48
  - type: UndistortElement
  - type: CustomPublisher
`;
  const metadata = `image_width: 64
image_height: 48
camera_matrix: {rows: 3, cols: 3, data: [50.0, 0.0, 32.0, 0.0, 50.0, 24.0, 0.0, 0.0, 1.0]}
distortion_coefficients: {rows: 1, cols: 5, data: [-0.25, 0.0, 0.0, 0.0, 0.0]}
`;
  const detected = await detectWebCaps(); // probe first: it can reset GPU-backed canvases
  // A grid image so remapping is visible.
  const src = new OffscreenCanvas(64, 48);
  const g = src.getContext('2d', { willReadFrequently: true });
  g.fillStyle = '#000'; g.fillRect(0, 0, 64, 48);
  g.fillStyle = '#fff';
  for (let x = 0; x < 64; x += 8) g.fillRect(x, 0, 1, 48);
  for (let y = 0; y < 48; y += 8) g.fillRect(0, y, 64, 1);

  const out = {};
  for (const caps of ['canvas', detected]) {
    const plan = core.plan(yaml, { backend: 'web', caps, metadata });
    const impl = plan.chain[1].impl;
    const rt = await Runtime.start(plan, { core });
    const result = new Promise((res) => (rt.handles.get('custom_pub_0').onFrame = res));
    await rt.handles.get('custom_src_0').write(await createImageBitmap(src));
    const frame = await result;
    // Read the output back through a 2D canvas (works for both WebGPU and CPU outputs).
    const rb = new OffscreenCanvas(64, 48);
    rb.getContext('2d').drawImage(frame, 0, 0);
    const d = rb.getContext('2d').getImageData(0, 0, 64, 48).data;
    const o = g.getImageData(0, 0, 64, 48).data;
    let diff = 0;
    for (let i = 0; i < d.length; i += 4) diff += Math.abs(d[i] - o[i]);
    const centre = (24 * 64 + 32) * 4;
    out[impl + ' [' + caps + ']'] = { diff, centreSame: d[centre] === o[centre] };
    await rt.stop();
  }
  return out;
});
console.log('undistort:', JSON.stringify(undistort));
await browser.close();
server.close();

if (errors.length) { console.error('page errors:', errors); process.exit(1); }
if (result.nonzero < 1000 || result.sink < 5) { console.error('FAIL: no video reached the sinks'); process.exit(1); }
for (const [impl, r] of Object.entries(undistort)) {
  if (typeof r === 'string') { console.log(`skip ${impl}: ${r}`); continue; }
  if (r.diff < 500) { console.error(`FAIL: ${impl} left the image unchanged`); process.exit(1); }
}
if (!Object.keys(undistort).some((k) => k.startsWith('undistort-cpu'))) { console.error('FAIL: cpu undistort did not run'); process.exit(1); }
console.log('browser OK');
