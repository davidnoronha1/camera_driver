/* Toy native plugin used by the tests: one lowered element, one without a web
 * implementation, a runner, and lifecycle hooks that log to a buffer. */
#include "camera_driver_plugin.h"
#include <stdio.h>
#include <string.h>

static char g_launch[1024];
static char g_events[1024];
static char g_text[512];
static int g_session;

CD_EXPORT const char *toy_events(void) { return g_events; }
CD_EXPORT const char *toy_last_launch(void) { return g_launch; }

static void log_event(const char *e) {
  size_t n = strlen(g_events);
  snprintf(g_events + n, sizeof g_events - n, "%s;", e);
}

/* ── ToyTag ───────────────────────────────────────────────────────────── */

static const char *const toy_required[] = {"tag", NULL};

static size_t toy_prefs(const cd_node *node, int32_t *out, size_t cap) {
  (void)node;
  if (cap < 2) return 0;
  out[0] = CD_FMT_RGB;
  out[1] = CD_FMT_I420;
  return 2;
}

static int toy_lower(const cd_host_api *host, cd_lower_ctx *ctx, const cd_node *node,
                     const cd_caps *upstream, cd_lowered *out) {
  cd_str tag = {0};
  int64_t gain = 7;
  if (!host->props_get_string(node->props, "tag", &tag)) return 1;
  host->props_get_int(node->props, "gain", &gain);

  /* options arrive as a list too */
  size_t n = host->props_list_len(node->props, "labels");
  cd_str first = {0};
  if (n > 0) host->props_list_string(node->props, "labels", 0, &first);

  /* Valid gst syntax (an `identity` with an informative name) so the same
   * string can run on a real GStreamer. */
  int n_written = snprintf(g_text, sizeof g_text, "identity name=%s_tag_%.*s_g%lld_l%zu_%.*s%s",
                           node->name, (int)tag.len, tag.ptr, (long long)gain, n, (int)first.len,
                           first.ptr ? first.ptr : "", host->has_element(ctx, "nvh264enc") ? " silent=true" : "");
  if (tag.len == 4 && memcmp(tag.ptr, "warn", 4) == 0) host->warn(ctx, "tag looks suspicious");
  host->add_handle(ctx, "toy_handle", CD_HANDLE_ELEMENT, node->type_name);

  out->text.ptr = g_text;
  out->text.len = (size_t)n_written;
  out->out = *upstream; /* passes caps through */
  return 0;
}

static void *toy_create(const cd_node *node) {
  (void)node;
  log_event("create");
  return &g_session;
}
static int toy_setup(void *self, void *pipeline) {
  log_event(self == &g_session && pipeline ? "setup" : "setup-bad");
  return 0;
}
static void toy_bringdown(void *self, void *pipeline) {
  (void)self;
  log_event(pipeline ? "bringdown" : "bringdown-bad");
}
static void toy_destroy(void *self) {
  (void)self;
  log_event("destroy");
}

static const cd_element_def toy_tag = {
    .type_name = "ToyTag",
    .name_prefix = "toy_tag",
    .role = CD_ROLE_TRANSFORM,
    .backends = CD_BACKEND_GST | CD_BACKEND_WEB,
    .required = toy_required,
    .preferred_inputs = toy_prefs,
    .lower = toy_lower,
    .web_impl = "toy-tag",
    .create = toy_create,
    .setup = toy_setup,
    .bringdown = toy_bringdown,
    .destroy = toy_destroy,
};

/* ── ToyNoWeb: lowers for gst only ────────────────────────────────────── */

static int nw_lower(const cd_host_api *host, cd_lower_ctx *ctx, const cd_node *node,
                    const cd_caps *upstream, cd_lowered *out) {
  (void)host; (void)ctx; (void)node;
  static const char t[] = "queue";
  out->text.ptr = t;
  out->text.len = sizeof t - 1;
  out->out = *upstream;
  return 0;
}

static const cd_element_def toy_noweb = {
    .type_name = "ToyNoWeb",
    .name_prefix = "toy_noweb",
    .role = CD_ROLE_TRANSFORM,
    .backends = CD_BACKEND_GST,
    .lower = nw_lower,
    .web_reason = "toy has no web version",
};

/* ── runner ───────────────────────────────────────────────────────────── */

static void *r_build(void *user, const cd_plan_view *plan, char *err, size_t err_cap) {
  (void)user;
  snprintf(g_launch, sizeof g_launch, "%.*s", (int)plan->launch.len, plan->launch.ptr);
  if (strstr(g_launch, "FAIL")) {
    snprintf(err, err_cap, "toy runner refuses this pipeline");
    return NULL;
  }
  return &g_session;
}
static void *r_native(void *s) { (void)s; return (void *)&g_launch; }
static void *r_find(void *s, const char *name) { (void)s; (void)name; return NULL; }
static int r_play(void *s) { (void)s; log_event("play"); return 0; }
static int r_wait(void *s) { (void)s; log_event("wait"); return 0; }
static void r_stop(void *s) { (void)s; }
static void r_destroy(void *s) { (void)s; log_event("runner-destroy"); }

static const cd_runner_def toy_runner = {
    .name = "toy-runner",
    .backend = "gst",
    .build = r_build,
    .native_handle = r_native,
    .find_element = r_find,
    .play = r_play,
    .wait = r_wait,
    .stop = r_stop,
    .destroy = r_destroy,
};

CD_EXPORT int cd_plugin_init(const cd_host_api *host) {
  if (host->abi_version != CD_ABI_VERSION) return -100;
  if (host->register_element(&toy_tag) != 0) return 1;
  if (host->register_element(&toy_noweb) != 0) return 2;
  if (host->register_runner(&toy_runner) != 0) return 3;
  return 0;
}
