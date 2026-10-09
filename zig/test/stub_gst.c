/* A stand-in for libgstreamer + libglib exporting just the symbols
 * runner_gst.zig resolves. It records what it was asked to do so the runner's
 * call sequence can be tested without GStreamer installed. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define EXPORT __attribute__((visibility("default")))

typedef struct { uint32_t domain; int code; const char *message; } GError;

static char g_launch[2048];
static char g_calls[512];
static int g_state;
static int g_eos_polls;
static int g_bus_marker, g_pipe_marker;
/* Mirrors the public GstMessage header: a 64-byte GstMiniObject, then `type`. */
typedef struct { char mini_object[64]; unsigned type; } Msg;
static Msg g_eos_msg = {{0}, 1};
static Msg g_err_msg = {{0}, 2};
static GError g_err = {1, 42, "boom"};
static GError g_parse_err = {1, 7, "no element \"nonsense\""};

static void note(const char *c) {
  size_t n = strlen(g_calls);
  snprintf(g_calls + n, sizeof g_calls - n, "%s;", c);
}

EXPORT const char *stub_last_launch(void) { return g_launch; }
EXPORT const char *stub_calls(void) { return g_calls; }
EXPORT int stub_state(void) { return g_state; }
EXPORT void stub_reset(void) { g_calls[0] = 0; g_launch[0] = 0; g_state = 0; g_eos_polls = 0; }

EXPORT void gst_init(int *argc, void *argv) { (void)argc; (void)argv; note("init"); }

EXPORT void *gst_parse_launch(const char *desc, GError **err) {
  snprintf(g_launch, sizeof g_launch, "%s", desc);
  note("parse");
  if (strstr(desc, "nonsense")) { *err = &g_parse_err; return NULL; }
  return &g_pipe_marker;
}
EXPORT int gst_element_set_state(void *el, int state) {
  (void)el; g_state = state;
  note(state == 4 ? "PLAYING" : "NULL");
  return state == 4 && strstr(g_launch, "NOPLAY") ? 0 : 1;
}
EXPORT void *gst_element_get_bus(void *el) { (void)el; return &g_bus_marker; }

EXPORT void *gst_bus_timed_pop_filtered(void *bus, uint64_t timeout, unsigned types) {
  (void)bus; (void)timeout;
  /* like the real one: only the first matching message is returned */
  if (strstr(g_launch, "ERRORPIPE")) return (types & 2) ? &g_err_msg : NULL;
  return (types & 1) && ++g_eos_polls >= 2 ? &g_eos_msg : NULL;
}
EXPORT void gst_message_parse_error(void *msg, GError **err, char **dbg) { (void)msg; *err = &g_err; *dbg = NULL; }
EXPORT void gst_message_unref(void *msg) { (void)msg; }
EXPORT void gst_object_unref(void *o) { (void)o; note("unref"); }
EXPORT void *gst_bin_get_by_name(void *bin, const char *name) {
  (void)bin;
  return strcmp(name, "known") == 0 ? &g_pipe_marker : NULL;
}
EXPORT void g_error_free(GError *e) { (void)e; }
EXPORT void g_free(void *p) { (void)p; }
