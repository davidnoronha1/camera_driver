// Smoke test for the wasm core, run under plain Node (no browser needed):
//   zig build wasm && node web/test_core.mjs
import { readFileSync } from 'node:fs';
import { Core } from './host.js';
import assert from 'node:assert/strict';

const here = new URL('.', import.meta.url);
const core = await Core.load(readFileSync(new URL('camera_driver.wasm', here)));
const cfg = (name) => readFileSync(new URL(`../../config/${name}`, here), 'utf8');

// 1. gst backend reproduces the strings the C++ elements emit.
const p1 = core.plan(cfg('example_pipeline.yaml'), { backend: 'gst' });
assert.equal(p1.ok, true);
assert.match(p1.launch, /^v4l2src device=\/dev\/video0 .* ! x264enc tune=zerolatency speed-preset=ultrafast bitrate=4000$/);

const p2 = core.plan(cfg('orbbec_lucid_pipeline.yaml'), { backend: 'gst', doc: 2, caps: 'nvunixfdsrc' });
assert.match(p2.launch, /^nvunixfdsrc socket-path=\/tmp\/camera_nv.sock /);

// 2. hardware flags change the pipeline.
const nv = core.plan(cfg('example_pipeline.yaml'), { backend: 'gst', caps: 'nvh264enc,x264enc,jpegenc' });
assert.equal(nv.ok, true);

// 3. web backend: native-only parts are reported, not dropped.
const w = core.plan(cfg('orbbec_lucid_pipeline.yaml'), { backend: 'web', doc: 0, caps: 'camera,canvas,jpeg_encode' });
assert.equal(w.ok, false);
assert.ok(w.unsupported.some((u) => u.type === 'IceOryxPublisher'));
assert.equal(w.chain[0].impl, 'camera');

// 4. structured errors.
const bad = core.plan('elements:\n  - type: Nope\n');
assert.equal(bad.ok, false);
assert.equal(bad.stage, 'graph');
const badYaml = core.plan('a: [1,\n');
assert.equal(badYaml.stage, 'yaml');
assert.equal(badYaml.line, 1);

// 5. metadata seeds undistort.
const u = core.plan('elements:\n  - type: CustomSrcElement\n    format: RGB\n  - type: UndistortElement\n', {
  backend: 'web',
  caps: 'canvas',
  metadata: `image_width: 8
image_height: 6
camera_matrix: {rows: 3, cols: 3, data: [10.0, 0.0, 4.0, 0.0, 10.0, 3.0, 0.0, 0.0, 1.0]}
distortion_coefficients: {rows: 1, cols: 5, data: [-0.3, 0.0, 0.0, 0.0, 0.0]}
`,
});
assert.equal(u.chain[1].impl, 'undistort-cpu');

// 6. CPU undistort kernel: zero distortion is the identity; barrel moves samples.
core.undistortInit({ width: 8, height: 6, K: [10, 0, 4, 0, 10, 3, 0, 0, 1], D: [0, 0, 0, 0, 0] });
const img = new Uint8ClampedArray(8 * 6 * 4).map((_, i) => i % 251);
assert.deepEqual(core.undistortFrame(img), img);
core.undistortInit({ width: 8, height: 6, K: [10, 0, 4, 0, 10, 3, 0, 0, 1], D: [-0.3, 0, 0, 0, 0] });
const moved = core.undistortFrame(img);
assert.notDeepEqual(moved, img);

// 7. web plugin backend: a manifest adds element types at runtime.
const before = core.listElements().length;
const pluginName = core.registerManifest({
  plugin: 'demo',
  elements: [
    {
      type: 'TimeOverlay', prefix: 'time_overlay', role: 'transform', required: ['font'],
      gst: { template: 'timeoverlay name={name} font-desc="{font}" valignment={valign|top}' },
      web: { impl: 'overlay-time' },
    },
    { type: 'GstOnlyThing', prefix: 'gst_only', role: 'sink', gst: { template: 'fakesink name={name}' }, web: { unsupported: 'nothing to render to' } },
  ],
});
assert.equal(pluginName, 'demo');
assert.equal(core.listElements().length, before + 2);
assert.equal(core.listElements().find((e) => e.type === 'TimeOverlay').origin, 'demo');

const cfgPlug = 'elements:\n  - type: CustomSrcElement\n    format: I420\n  - type: TimeOverlay\n    font: Sans 20\n';
const g = core.plan(cfgPlug, { backend: 'gst' });
assert.match(g.launch, /timeoverlay name=time_overlay_0 font-desc="Sans 20" valignment=top$/);
const wp = core.plan(cfgPlug, { backend: 'web', caps: 'canvas' });
assert.equal(wp.ok, true);
assert.equal(wp.chain[1].impl, 'overlay-time');
const wp2 = core.plan(cfgPlug + '  - type: GstOnlyThing\n', { backend: 'web', caps: 'canvas' });
assert.equal(wp2.ok, false);
assert.equal(wp2.unsupported[0].reason, 'nothing to render to');

// rejected manifests say why, and don't half-register
assert.throws(() => core.registerManifest({ elements: [{ type: 'MuxElement', prefix: 't', role: 'mux' }] }), /already registered/);
assert.throws(() => core.registerManifest('not json'), /not valid JSON/);
assert.throws(() => core.registerManifest({ elements: [{ type: 'Bad', prefix: 'b', role: 'nope' }] }), /role/);
assert.equal(core.plan('elements:\n  - type: Bad\n').stage, 'graph'); // 'Bad' never registered

console.log('wasm core OK (' + readFileSync(new URL('camera_driver.wasm', here)).length + ' bytes)');
