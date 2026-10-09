// Browser host for the camera_driver wasm core.
//
// Two jobs:
//   1. Core        - thin wrapper around camera_driver.wasm: config text in,
//                    plan (gst or web) out, plus the CPU undistort kernel.
//   2. Runtime     - executes a *web* plan with browser APIs. The plan's
//                    stage `impl` ids are the contract with the Zig web
//                    factory (zig/src/web_factory.zig); this file implements
//                    them.
//
// Frames flowing between stages are plain browser objects: VideoFrame,
// ImageBitmap or an OffscreenCanvas for raw video, `{encoded:'jpeg', blob}` /
// `{encoded:'h264', chunk}` for compressed video.
//
// Ownership: whoever receives a VideoFrame/ImageBitmap closes it once done
// (a camera's MediaStreamTrackProcessor stalls if frames are never closed).
// `tee` hands each branch its own clone.

// ─── core ────────────────────────────────────────────────────────────────────

const enc = new TextEncoder();
const dec = new TextDecoder();

export class Core {
  constructor(exports) {
    this.x = exports;
  }

  static async load(source) {
    let bytes = source;
    if (typeof source === 'string' || source instanceof URL) {
      bytes = await (await fetch(source)).arrayBuffer();
    } else if (source instanceof Response) {
      bytes = await source.arrayBuffer();
    }
    const { instance } = await WebAssembly.instantiate(bytes, {});
    return new Core(instance.exports);
  }

  _put(text) {
    const data = typeof text === 'string' ? enc.encode(text) : text;
    if (data.length === 0) return [0, 0];
    const ptr = this.x.cd_alloc(data.length);
    if (!ptr) throw new Error('wasm out of memory');
    new Uint8Array(this.x.memory.buffer, ptr, data.length).set(data);
    return [ptr, data.length];
  }

  /**
   * Plan a pipeline config.
   * @param {string} yaml
   * @param {{backend?: 'gst'|'web', doc?: number, caps?: string, metadata?: string}} opts
   *   caps: GStreamer element names (gst) or web capability names (web), comma separated.
   *   metadata: camera_info / metadata YAML text to seed the scratchpad.
   */
  plan(yaml, opts = {}) {
    const [sp, sl] = this._put(yaml);
    const [cp, cl] = this._put(opts.caps ?? '');
    const [mp, ml] = this._put(opts.metadata ?? '');
    try {
      const len = this.x.cd_plan(sp, sl, opts.backend === 'web' ? 1 : 0, opts.doc ?? 0, cp, cl, mp, ml);
      const ptr = this.x.cd_result_ptr();
      return JSON.parse(dec.decode(new Uint8Array(this.x.memory.buffer, ptr, len)));
    } finally {
      for (const [p, l] of [[sp, sl], [cp, cl], [mp, ml]]) if (l) this.x.cd_free(p, l);
    }
  }

  /**
   * Add element types from a plugin manifest (object or JSON text). This is
   * the web plugin backend's registration step; pair it with `registerImpl`
   * for the JS that realises each `web.impl`. Throws on a bad manifest.
   */
  registerManifest(manifest) {
    const text = typeof manifest === 'string' ? manifest : JSON.stringify(manifest);
    const [p, l] = this._put(text);
    try {
      const len = this.x.cd_register_manifest(p, l);
      const res = JSON.parse(dec.decode(new Uint8Array(this.x.memory.buffer, this.x.cd_result_ptr(), len)));
      if (!res.ok) throw new Error('manifest rejected: ' + res.error);
      return res.plugin;
    } finally {
      if (l) this.x.cd_free(p, l);
    }
  }

  /** [{type, role, origin, gst, web}] for every registered element type. */
  listElements() {
    const len = this.x.cd_list_elements();
    return JSON.parse(dec.decode(new Uint8Array(this.x.memory.buffer, this.x.cd_result_ptr(), len)));
  }

  /** Prepare the CPU undistort kernel. `cal` = {width,height,K:[9],D:[k1,k2,p1,p2,k3]}. */
  undistortInit(cal) {
    const [fx, cx, fy, cy] = [cal.K[0], cal.K[2], cal.K[4], cal.K[5]];
    const d = (i) => cal.D[i] ?? 0;
    if (!this.x.cd_undistort_init(cal.width, cal.height, fx, fy, cx, cy, d(0), d(1), d(2), d(3), d(4)))
      throw new Error('undistort init failed');
    const n = cal.width * cal.height * 4;
    if (this._ud) {
      this.x.cd_free(this._ud.src, this._ud.n);
      this.x.cd_free(this._ud.dst, this._ud.n);
    }
    this._ud = { n, src: this.x.cd_alloc(n), dst: this.x.cd_alloc(n), w: cal.width, h: cal.height };
  }

  /** RGBA bytes in -> RGBA bytes out (same size as undistortInit). */
  undistortFrame(rgba) {
    const u = this._ud;
    if (!u) throw new Error('undistortInit() first');
    new Uint8Array(this.x.memory.buffer, u.src, u.n).set(rgba.subarray(0, u.n));
    this.x.cd_undistort_frame(u.src, u.dst);
    return new Uint8ClampedArray(new Uint8Array(this.x.memory.buffer, u.dst, u.n).slice());
  }
}

/** Probe the browser and return the capability CSV the planner expects. */
export async function detectWebCaps(extra = []) {
  const caps = new Set(extra);
  if (navigator.mediaDevices?.getUserMedia) caps.add('camera');
  if (typeof OffscreenCanvas !== 'undefined' || typeof document !== 'undefined') caps.add('canvas');
  if (typeof OffscreenCanvas !== 'undefined' && OffscreenCanvas.prototype.convertToBlob) caps.add('jpeg_encode');
  if (typeof fetch === 'function' && typeof ReadableStream !== 'undefined') caps.add('fetch_stream');
  if (typeof VideoEncoder !== 'undefined') {
    try {
      const r = await VideoEncoder.isConfigSupported({ codec: 'avc1.42001f', width: 640, height: 480 });
      if (r.supported) caps.add('webcodecs_h264_encode');
    } catch {}
  }
  if (typeof VideoDecoder !== 'undefined') {
    try {
      const r = await VideoDecoder.isConfigSupported({ codec: 'avc1.42001f' });
      if (r.supported) caps.add('webcodecs_h264_decode');
    } catch {}
  }
  if (await webgpuWorks()) caps.add('webgpu');
  return [...caps].join(',');
}

/**
 * Having a WebGPU adapter is not enough: some software/headless stacks hand
 * one out but cannot present a canvas readable by drawImage(). Render a
 * known colour and read it back before advertising the capability, so the
 * planner falls back to the wasm undistort kernel instead of showing black.
 */
export async function webgpuWorks() {
  try {
    if (!navigator.gpu || typeof OffscreenCanvas === 'undefined') return false;
    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) return false;
    const device = await adapter.requestDevice();
    const canvas = new OffscreenCanvas(4, 4);
    const ctx = canvas.getContext('webgpu');
    if (!ctx) return false;
    ctx.configure({ device, format: navigator.gpu.getPreferredCanvasFormat(), alphaMode: 'opaque' });
    const enc = device.createCommandEncoder();
    enc.beginRenderPass({
      colorAttachments: [{ view: ctx.getCurrentTexture().createView(), clearValue: { r: 1, g: 0, b: 0, a: 1 }, loadOp: 'clear', storeOp: 'store' }],
    }).end();
    device.queue.submit([enc.finish()]);
    const rb = new OffscreenCanvas(4, 4).getContext('2d', { willReadFrequently: true });
    rb.drawImage(canvas, 0, 0);
    const ok = rb.getImageData(0, 0, 1, 1).data[0] > 200;
    device.destroy();
    return ok;
  } catch {
    return false;
  }
}

// ─── runtime ─────────────────────────────────────────────────────────────────

const frameSize = (f) => ({
  w: f.displayWidth ?? f.videoWidth ?? f.width,
  h: f.displayHeight ?? f.videoHeight ?? f.height,
});

async function toDrawable(item) {
  if (item && item.encoded === 'jpeg') return createImageBitmap(item.blob);
  return item;
}

/** Stage implementations. Each returns {push(item), start?(), stop?()} and receives `emit` to forward. */
const impls = Object.create(null);
Object.assign(impls, {
  // ── sources ──
  async camera(stage, emit, ctx) {
    const { width, height, fps } = { width: stage.out.width, height: stage.out.height, fps: stage.out.fps };
    const video = {};
    if (width) video.width = { ideal: width };
    if (height) video.height = { ideal: height };
    if (fps) video.frameRate = { ideal: fps };
    const sel = stage.props.selection?.find?.((s) => s.mode === 'serial' || s.mode === 'device');
    if (sel?.device_id) video.deviceId = { exact: sel.device_id };
    let stream, reader, running = false;
    return {
      async start() {
        stream = await navigator.mediaDevices.getUserMedia({ video, audio: false });
        const track = stream.getVideoTracks()[0];
        running = true;
        if (typeof MediaStreamTrackProcessor !== 'undefined') {
          reader = new MediaStreamTrackProcessor({ track }).readable.getReader();
          (async () => {
            while (running) {
              const { value, done } = await reader.read();
              if (done || !value) break;
              await emit(value);
              value.close?.();
            }
          })();
        } else {
          // Fallback for browsers without insertable streams.
          const el = Object.assign(document.createElement('video'), { srcObject: stream, muted: true, playsInline: true });
          await el.play();
          const tick = async () => {
            if (!running) return;
            await emit(await createImageBitmap(el));
            el.requestVideoFrameCallback ? el.requestVideoFrameCallback(tick) : setTimeout(tick, 33);
          };
          tick();
        }
      },
      stop() {
        running = false;
        reader?.cancel().catch(() => {});
        stream?.getTracks().forEach((t) => t.stop());
      },
    };
  },

  async 'push-source'(stage, emit, ctx) {
    // Application code feeds frames with handles.get(name).write(frame).
    ctx.handles.set(stage.name, { kind: 'frame_source', write: (frame) => emit(frame) });
    return {};
  },

  async 'http-mjpeg-source'(stage, emit) {
    const ctl = new AbortController();
    return {
      async start() {
        const res = await fetch(stage.props.url, { signal: ctl.signal });
        const reader = res.body.getReader();
        let buf = new Uint8Array(0);
        (async () => {
          for (;;) {
            const { value, done } = await reader.read();
            if (done) break;
            const joined = new Uint8Array(buf.length + value.length);
            joined.set(buf);
            joined.set(value, buf.length);
            buf = joined;
            // Frames are JPEGs: SOI FFD8 ... EOI FFD9. Scan for complete ones.
            for (;;) {
              const s = indexOf2(buf, 0xff, 0xd8, 0);
              if (s < 0) break;
              const e = indexOf2(buf, 0xff, 0xd9, s + 2);
              if (e < 0) break;
              const jpeg = buf.slice(s, e + 2);
              buf = buf.slice(e + 2);
              await emit({ encoded: 'jpeg', blob: new Blob([jpeg], { type: 'image/jpeg' }) });
            }
          }
        })().catch(() => {});
      },
      stop: () => ctl.abort(),
    };
  },

  // ── transforms ──
  async 'encode-jpeg'(stage, emit) {
    const { quality = 85, resize } = stage.config ?? {};
    let canvas = null, busy = false;
    return {
      async push(item) {
        if (busy) return item.close?.(); // drop rather than queue: live video
        busy = true;
        try {
          const src = await toDrawable(item);
          const { w, h } = frameSize(src);
          const W = resize?.width > 0 ? resize.width : w, H = resize?.height > 0 ? resize.height : h;
          if (!canvas || canvas.width !== W || canvas.height !== H) canvas = new OffscreenCanvas(W, H);
          canvas.getContext('2d').drawImage(src, 0, 0, W, H);
          src.close?.();
          const blob = await canvas.convertToBlob({ type: 'image/jpeg', quality: quality / 100 });
          await emit({ encoded: 'jpeg', blob });
        } finally {
          busy = false;
        }
      },
    };
  },

  async 'encode-h264'(stage, emit) {
    const { bitrate_kbps = 0, resize } = stage.config ?? {};
    let encoder = null, size = '';
    return {
      async push(item) {
        const src = await toDrawable(item);
        const { w, h } = frameSize(src);
        const W = resize?.width > 0 ? resize.width : w, H = resize?.height > 0 ? resize.height : h;
        if (!encoder || size !== `${W}x${H}`) {
          encoder?.close();
          size = `${W}x${H}`;
          encoder = new VideoEncoder({
            output: (chunk) => emit({ encoded: 'h264', chunk }),
            error: (e) => console.error('h264 encode', e),
          });
          encoder.configure({
            codec: 'avc1.42001f', width: W, height: H,
            bitrate: bitrate_kbps > 0 ? bitrate_kbps * 1000 : 2_000_000,
            latencyMode: 'realtime', avc: { format: 'annexb' },
          });
        }
        const frame = src instanceof VideoFrame ? src : new VideoFrame(src, { timestamp: performance.now() * 1000 });
        encoder.encode(frame);
        if (frame !== src) frame.close();
        src.close?.();
      },
      stop: () => encoder?.close(),
    };
  },

  async convert(stage, emit) {
    // Browser frames are format-agnostic; the only real work is decoding JPEG.
    return { push: async (item) => emit(await toDrawable(item)) };
  },

  async 'undistort-cpu'(stage, emit, ctx) {
    const cal = stage.config;
    ctx.core.undistortInit(cal);
    const canvas = new OffscreenCanvas(cal.width, cal.height);
    const g = canvas.getContext('2d', { willReadFrequently: true });
    return {
      async push(item) {
        const src = await toDrawable(item);
        g.drawImage(src, 0, 0, cal.width, cal.height);
        const img = g.getImageData(0, 0, cal.width, cal.height);
        src.close?.();
        const out = ctx.core.undistortFrame(img.data);
        g.putImageData(new ImageData(out, cal.width, cal.height), 0, 0);
        await emit(canvas);
      },
    };
  },

  async 'undistort-webgpu'(stage, emit) {
    const cal = stage.config;
    const adapter = await navigator.gpu.requestAdapter();
    const device = await adapter.requestDevice();
    const canvas = new OffscreenCanvas(cal.width, cal.height);
    const ctx = canvas.getContext('webgpu');
    const format = navigator.gpu.getPreferredCanvasFormat();
    ctx.configure({ device, format, alphaMode: 'opaque' });

    const d = (i) => cal.D[i] ?? 0;
    const params = new Float32Array([
      cal.K[0], cal.K[4], cal.K[2], cal.K[5],  // fx fy cx cy
      d(0), d(1), d(2), d(3),                  // k1 k2 p1 p2
      d(4), cal.width, cal.height, 0,          // k3 w h pad
    ]);
    const ubo = device.createBuffer({ size: params.byteLength, usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST });
    device.queue.writeBuffer(ubo, 0, params);

    const module = device.createShaderModule({
      code: /* wgsl */ `
        struct P { a: vec4f, b: vec4f, c: vec4f };
        @group(0) @binding(0) var<uniform> p: P;
        @group(0) @binding(1) var tex: texture_2d<f32>;
        @group(0) @binding(2) var smp: sampler;
        @vertex fn vs(@builtin(vertex_index) i: u32) -> @builtin(position) vec4f {
          var q = array<vec2f, 3>(vec2f(-1, -1), vec2f(3, -1), vec2f(-1, 3));
          return vec4f(q[i], 0, 1);
        }
        @fragment fn fs(@builtin(position) pos: vec4f) -> @location(0) vec4f {
          let fx = p.a.x; let fy = p.a.y; let cx = p.a.z; let cy = p.a.w;
          let x = (pos.x - 0.5 - cx) / fx; let y = (pos.y - 0.5 - cy) / fy;
          let r2 = x * x + y * y;
          let rad = 1.0 + p.b.x * r2 + p.b.y * r2 * r2 + p.c.x * r2 * r2 * r2;
          let xd = x * rad + 2.0 * p.b.z * x * y + p.b.w * (r2 + 2.0 * x * x);
          let yd = y * rad + p.b.z * (r2 + 2.0 * y * y) + 2.0 * p.b.w * x * y;
          let uv = vec2f(fx * xd + cx + 0.5, fy * yd + cy + 0.5) / vec2f(p.c.y, p.c.z);
          if (uv.x < 0.0 || uv.y < 0.0 || uv.x > 1.0 || uv.y > 1.0) { return vec4f(0, 0, 0, 1); }
          return textureSampleLevel(tex, smp, uv, 0.0);
        }`,
    });
    const pipeline = device.createRenderPipeline({
      layout: 'auto',
      vertex: { module, entryPoint: 'vs' },
      fragment: { module, entryPoint: 'fs', targets: [{ format }] },
    });
    const texture = device.createTexture({
      size: [cal.width, cal.height], format: 'rgba8unorm',
      usage: GPUTextureUsage.TEXTURE_BINDING | GPUTextureUsage.COPY_DST | GPUTextureUsage.RENDER_ATTACHMENT,
    });
    const sampler = device.createSampler({ magFilter: 'linear', minFilter: 'linear' });
    const bind = device.createBindGroup({
      layout: pipeline.getBindGroupLayout(0),
      entries: [
        { binding: 0, resource: { buffer: ubo } },
        { binding: 1, resource: texture.createView() },
        { binding: 2, resource: sampler },
      ],
    });
    return {
      async push(item) {
        const src = await toDrawable(item);
        device.queue.copyExternalImageToTexture({ source: src }, { texture }, [cal.width, cal.height]);
        src.close?.();
        const enc = device.createCommandEncoder();
        const pass = enc.beginRenderPass({
          colorAttachments: [{ view: ctx.getCurrentTexture().createView(), loadOp: 'clear', storeOp: 'store' }],
        });
        pass.setPipeline(pipeline);
        pass.setBindGroup(0, bind);
        pass.draw(3);
        pass.end();
        device.queue.submit([enc.finish()]);
        await emit(canvas);
      },
      stop: () => device.destroy(),
    };
  },

  // ── fan-out ──
  async tee(stage, emit, ctx) {
    // Branch wiring is done by the runtime; tee just forwards to every branch.
    return { push: (item) => emit(item) };
  },

  // ── sinks ──
  async 'canvas-display'(stage, emit, ctx) {
    const canvas = ctx.opts.canvas;
    if (!canvas) throw new Error("canvas-display needs Runtime option 'canvas'");
    const g = canvas.getContext('2d');
    let frames = 0, t0 = performance.now();
    return {
      async push(item) {
        const src = await toDrawable(item);
        const { w, h } = frameSize(src);
        if (canvas.width !== w || canvas.height !== h) Object.assign(canvas, { width: w, height: h });
        g.drawImage(src, 0, 0);
        src.close?.();
        frames++;
        const now = performance.now();
        if (now - t0 > 1000) {
          ctx.opts.onStats?.({ fps: (frames * 1000) / (now - t0), width: w, height: h });
          frames = 0;
          t0 = now;
        }
      },
    };
  },

  async 'callback-sink'(stage, emit, ctx) {
    const handle = { kind: 'frame_sink', onFrame: null };
    ctx.handles.set(stage.name, handle);
    // onFrame owns the frame: call frame.close() on VideoFrame/ImageBitmap when done.
    return { push: async (item) => (handle.onFrame ? handle.onFrame(item) : item.close?.()) };
  },
});

/**
 * Register the JS that realises a plan stage `impl` id. The factory function
 * is `async (stage, emit, ctx) => ({push?, start?, stop?})`:
 *   stage  the plan stage ({name, type, props, config, in, out, ...})
 *   emit   forwards an item to the next stage
 *   ctx    {handles: Map, core, opts}; add entries to `handles` to expose
 *          write()/onFrame() style attachment points to application code
 * Overriding an existing id replaces it (e.g. a custom camera source).
 */
export function registerImpl(id, factory) {
  impls[id] = factory;
}

export class UnsupportedPlanError extends Error {
  constructor(plan) {
    super('plan has unsupported elements: ' + plan.unsupported.map((u) => `${u.name} (${u.reason})`).join('; '));
    this.unsupported = plan.unsupported;
  }
}

// ─── runners (the swappable "finalizer") ─────────────────────────────────────
//
// A runner executes a lowered plan. Same contract as the native
// `cd_runner_def` in include/camera_driver_plugin.h:
//
//   build(plan, opts)   -> session      create the pipeline (nothing flows yet)
//   handles(session)    -> Map          named attachment points (sources/sinks)
//   play(session)       -> Promise      start flowing
//   wait(session)       -> Promise<boolean>   resolves true on clean end / stop
//   stop(session)       -> Promise
//   destroy(session)    -> Promise
//
// `registerRunner('name', runner)` adds one; `Runtime.start(plan, {runner:'name'})`
// picks it. The default is 'web', defined below.

const runners = new Map();

export function registerRunner(name, runner) {
  runners.set(name, runner);
}
export function listRunners() {
  return [...runners.keys()];
}

/** The default browser runner: instantiates each stage's registered `impl` and wires them push-style. */
const webRunner = {
  async build(plan, opts) {
    const session = { stages: [], handles: new Map(), done: null, finish: null };
    session.done = new Promise((res) => (session.finish = res));
    session.ctx = { handles: session.handles, core: opts.core, opts };
    session.head = await buildChain(session, plan.chain, async () => {});
    return session;
  },
  handles: (session) => session.handles,
  async play(session) {
    // Sinks were built first, so start them first and sources last.
    for (const st of session.stages) await st.start?.();
  },
  wait: (session) => session.done,
  async stop(session) {
    for (const st of session.stages) await st.stop?.();
    session.finish(true);
  },
  async destroy() {},
};
registerRunner('web', webRunner);

// Build a chain back-to-front so each stage knows its downstream `emit`.
async function buildChain(session, chain, tail) {
  let next = tail;
  for (let i = chain.length - 1; i >= 0; i--) {
    const stage = chain[i];
    const impl = impls[stage.impl];
    if (!impl) throw new Error(`host has no implementation of '${stage.impl}'`);

    let emit = next;
    if (stage.branches) {
      const branchHeads = [];
      for (const b of stage.branches) branchHeads.push(await buildChain(session, b, async () => {}));
      emit = async (item) => {
        // A frame can be a single-use VideoFrame; give each branch its own clone.
        await Promise.all(branchHeads.map((h) => h(typeof VideoFrame !== 'undefined' && item instanceof VideoFrame ? item.clone() : item)));
        item.close?.();
      };
    }
    const built = await impl(stage, emit, session.ctx);
    session.stages.push(built);
    next = built.push ? (item) => built.push(item) : emit;
  }
  return next;
}

export class Runtime {
  /**
   * @param {object} plan  result of core.plan(yaml, {backend:'web'})
   * @param {{core: Core, runner?: string, canvas?: HTMLCanvasElement, onStats?: Function}} opts
   */
  static async start(plan, opts) {
    if (!plan.ok) throw new UnsupportedPlanError(plan);
    const name = opts.runner ?? 'web';
    const runner = runners.get(name);
    if (!runner) throw new Error(`no runner named '${name}' (have: ${listRunners().join(', ')})`);

    const rt = new Runtime();
    rt.runner = runner;
    rt.session = await runner.build(plan, opts);
    rt.handles = runner.handles(rt.session);
    await runner.play(rt.session);
    return rt;
  }

  /** Resolves when the pipeline ends or is stopped. */
  wait() {
    return this.runner.wait(this.session);
  }

  async stop() {
    await this.runner.stop(this.session);
    await this.runner.destroy(this.session);
  }
}

// ─── web plugins ─────────────────────────────────────────────────────────────

/**
 * The web plugin backend. A plugin is an object (or an ES module exporting it):
 *
 *   { manifest: {plugin, elements: [...]},   // what the planner learns (see manifest.zig)
 *     impls:    { 'impl-id': factory },      // how each web.impl runs (see registerImpl)
 *     runners:  { name: runner } }           // optional swappable runners
 *
 * Pass the object, or a URL to `import()`.
 */
export async function loadWebPlugin(core, plugin) {
  const mod = typeof plugin === 'string' || plugin instanceof URL ? await import(/* @vite-ignore */ plugin) : plugin;
  const p = mod.default ?? mod;
  const name = p.manifest ? core.registerManifest(p.manifest) : (p.name ?? 'plugin');
  for (const [id, factory] of Object.entries(p.impls ?? {})) registerImpl(id, factory);
  for (const [n, r] of Object.entries(p.runners ?? {})) registerRunner(n, r);
  return name;
}

function indexOf2(buf, a, b, from) {
  for (let i = from; i < buf.length - 1; i++) if (buf[i] === a && buf[i + 1] === b) return i;
  return -1;
}
