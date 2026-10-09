/*
 * camera_driver plugin ABI (version 1)
 *
 * A native plugin is a shared library exporting one symbol:
 *
 *     int cd_plugin_init(const cd_host_api *host);
 *
 * which registers elements and/or runners through `host`. Everything crossing
 * this boundary is plain C: no C++ types, no exceptions, no Zig types. GStreamer
 * objects (GstPipeline*, GstElement*) cross as opaque `void*`.
 *
 * Two kinds of plugin objects:
 *
 *   element  a pipeline element: how to *lower* it to a GStreamer fragment
 *            (planning) and, optionally, hooks that run once the pipeline exists.
 *   runner   the "finalizer": takes the lowered plan and executes it (builds the
 *            GStreamer pipeline, plays it, waits). Swappable, so a desktop build
 *            can bring its own (e.g. a ROS-aware one) and the browser has its own.
 *
 * Strings passed to the plugin are valid for the duration of the call only; the
 * host copies any string the plugin returns.
 */
#ifndef CAMERA_DRIVER_PLUGIN_H
#define CAMERA_DRIVER_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CD_ABI_VERSION 1u

#if defined(_WIN32)
#define CD_EXPORT __declspec(dllexport)
#else
#define CD_EXPORT __attribute__((visibility("default")))
#endif

typedef struct cd_props cd_props;          /* element options (string-keyed map) */
typedef struct cd_lower_ctx cd_lower_ctx;  /* per-lowering state */

typedef enum {
  CD_ROLE_SOURCE = 0,
  CD_ROLE_TRANSFORM = 1,
  CD_ROLE_MUX = 2,
  CD_ROLE_SINK = 3
} cd_role;

enum { CD_BACKEND_GST = 1u << 0, CD_BACKEND_WEB = 1u << 1 };

/* Same order as caps.PixelFormat in the Zig core. */
typedef enum {
  CD_FMT_UNKNOWN = 0,
  CD_FMT_YUYV,
  CD_FMT_NV12,
  CD_FMT_NV12_NVMM,
  CD_FMT_I420,
  CD_FMT_RGB,
  CD_FMT_BGR,
  CD_FMT_RGBA,
  CD_FMT_MJPEG,
  CD_FMT_H264,
  CD_FMT_H264_NVMM,
  CD_FMT_BAYER_RGGB,
  CD_FMT_BAYER_BGGR,
  CD_FMT_BAYER_GRBG,
  CD_FMT_BAYER_GBRG
} cd_format;

typedef struct {
  int32_t format;  /* cd_format */
  int32_t width, height;
  int32_t fps_num, fps_den;
  uint8_t is_nvmm, is_any;
} cd_caps;

typedef struct {
  const char *ptr;
  size_t len;
} cd_str;

typedef struct {
  const char *type_name; /* e.g. "McapSinkElement" */
  const char *name;      /* unique instance name, e.g. "mcap_sink_0" */
  const cd_props *props;
} cd_node;

typedef struct {
  cd_str text;   /* gst launch fragment; empty = contributes nothing */
  cd_caps out;   /* caps leaving this element */
} cd_lowered;

typedef enum {
  CD_HANDLE_FRAME_SOURCE = 0,
  CD_HANDLE_FRAME_SINK = 1,
  CD_HANDLE_TOPIC_WRITER = 2,
  CD_HANDLE_TOPIC_READER = 3,
  CD_HANDLE_ELEMENT = 4
} cd_handle_kind;

typedef struct {
  const char *name;
  int32_t kind; /* cd_handle_kind */
  const char *type_name;
} cd_handle;

typedef struct {
  const char *path;
  cd_str contents;
} cd_side_file;

/* ── element definition ─────────────────────────────────────────────────── */

typedef struct cd_host_api cd_host_api;

typedef struct {
  const char *type_name;   /* the `type:` used in configs */
  const char *name_prefix; /* instance names are <prefix>_<n> */
  int32_t role;            /* cd_role */
  uint32_t backends;       /* CD_BACKEND_* this plugin lowers for */
  const char *const *required; /* NULL-terminated option names, or NULL */

  /* Writes up to `cap` preferred input formats into `out`, returns the count.
   * May be NULL (no preference). */
  size_t (*preferred_inputs)(const cd_node *node, int32_t *out, size_t cap);

  /* GStreamer lowering. Return 0 on success. */
  int (*lower)(const cd_host_api *host, cd_lower_ctx *ctx, const cd_node *node,
               const cd_caps *upstream, cd_lowered *out);

  /* Web backend: the `impl` id the browser host implements for this element,
   * or NULL with `web_reason` explaining why there is none. */
  const char *web_impl;
  const char *web_reason;

  /* Runtime hooks (desktop runners call these). All optional. `create` returns
   * an instance pointer passed to the others. `native_pipeline` is the
   * runner's native handle (a GstPipeline* for the gst runner). */
  void *(*create)(const cd_node *node);
  int (*setup)(void *self, void *native_pipeline);
  void (*bringdown)(void *self, void *native_pipeline);
  void (*destroy)(void *self);
} cd_element_def;

/* ── runner definition ──────────────────────────────────────────────────── */

typedef struct {
  const char *backend; /* "gst" */
  cd_str launch;
  const cd_handle *handles;
  size_t n_handles;
  const cd_side_file *side_files;
  size_t n_side_files;
} cd_plan_view;

typedef struct {
  const char *name; /* selected with --runner <name> */
  const char *backend;
  void *user;

  /* Build the native pipeline from the plan. Returns a session or NULL. */
  void *(*build)(void *user, const cd_plan_view *plan, char *err, size_t err_cap);
  /* The native object plugins' `setup` hooks receive (GstPipeline*). */
  void *(*native_handle)(void *session);
  /* Look up a named element in the built pipeline, or NULL. */
  void *(*find_element)(void *session, const char *name);
  int (*play)(void *session);
  /* Block until the pipeline ends. Returns 0 on EOS, nonzero on error. */
  int (*wait)(void *session);
  void (*stop)(void *session);
  void (*destroy)(void *session);
} cd_runner_def;

/* ── host API handed to cd_plugin_init ──────────────────────────────────── */

struct cd_host_api {
  uint32_t abi_version; /* CD_ABI_VERSION the host was built with */

  /* registration (valid during cd_plugin_init) */
  int (*register_element)(const cd_element_def *def);
  int (*register_runner)(const cd_runner_def *def);

  /* option access: return 1 if present with that type, else 0 */
  int (*props_get_string)(const cd_props *p, const char *key, cd_str *out);
  int (*props_get_int)(const cd_props *p, const char *key, int64_t *out);
  int (*props_get_float)(const cd_props *p, const char *key, double *out);
  int (*props_get_bool)(const cd_props *p, const char *key, int *out);
  size_t (*props_list_len)(const cd_props *p, const char *key);
  int (*props_list_string)(const cd_props *p, const char *key, size_t i, cd_str *out);

  /* lowering helpers */
  void (*add_handle)(cd_lower_ctx *ctx, const char *name, int32_t kind, const char *type_name);
  void (*warn)(cd_lower_ctx *ctx, const char *message);
  /* 1 if the named GStreamer element is known to exist on this host. */
  int (*has_element)(const cd_lower_ctx *ctx, const char *gst_element);
};

typedef int (*cd_plugin_init_fn)(const cd_host_api *host);

#ifdef __cplusplus
}
#endif

#endif /* CAMERA_DRIVER_PLUGIN_H */
