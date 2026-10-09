// Example C++ plugin: an ordinary C++ class wrapped behind the C ABI.
//
// This is the pattern for moving existing C++ elements (McapSinkElement,
// ROS2Publisher, ...) out of the core: keep the class as it is, add a thin
// extern "C" shim that adapts it to cd_element_def. Nothing C++ (std::string,
// exceptions, virtuals) crosses the boundary.
#include "camera_driver_plugin.h"

#include <cstdio>
#include <exception>
#include <string>

namespace {

// Stand-in for an existing element class with its own state and C++ types.
class GainElement {
 public:
  GainElement(std::string name, double gain) : name_(std::move(name)), gain_(gain) {}

  // What it contributes to the gst launch string.
  std::string gstFragment() const {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.3f", gain_);
    return "audioamplify name=" + name_ + " amplification=" + buf;  // illustrative
  }

  void onPipelineReady(void* /*GstPipeline* */) { ready_ = true; }
  bool ready() const { return ready_; }

 private:
  std::string name_;
  double gain_;
  bool ready_ = false;
};

const char* const kRequired[] = {"gain", nullptr};
std::string g_text;  // lowering results are copied by the host before the next call

int lower(const cd_host_api* host, cd_lower_ctx* ctx, const cd_node* node,
          const cd_caps* upstream, cd_lowered* out) {
  try {  // never let an exception cross the C boundary
    double gain = 1.0;
    if (!host->props_get_float(node->props, "gain", &gain)) {
      int64_t whole = 0;  // YAML `gain: 2` parses as an int
      if (!host->props_get_int(node->props, "gain", &whole)) return 1;
      gain = static_cast<double>(whole);
    }
    g_text = GainElement(node->name, gain).gstFragment();
    out->text = {g_text.data(), g_text.size()};
    out->out = *upstream;
    return 0;
  } catch (const std::exception& e) {
    host->warn(ctx, e.what());
    return 2;
  }
}

// Runtime instance: the same class, created when the runner builds the pipeline.
void* create(const cd_node* node) {
  try { return new GainElement(node->name, 1.0); } catch (...) { return nullptr; }
}
int setup(void* self, void* native_pipeline) {
  if (!self) return 1;
  static_cast<GainElement*>(self)->onPipelineReady(native_pipeline);
  return 0;
}
void destroy(void* self) { delete static_cast<GainElement*>(self); }

const cd_element_def kGain = {
    /*type_name*/ "CppGain",
    /*name_prefix*/ "cpp_gain",
    /*role*/ CD_ROLE_TRANSFORM,
    /*backends*/ CD_BACKEND_GST,
    /*required*/ kRequired,
    /*preferred_inputs*/ nullptr,
    /*lower*/ lower,
    /*web_impl*/ nullptr,
    /*web_reason*/ "audio-only example element",
    /*create*/ create,
    /*setup*/ setup,
    /*bringdown*/ nullptr,
    /*destroy*/ destroy,
};

}  // namespace

extern "C" CD_EXPORT int cd_plugin_init(const cd_host_api* host) {
  if (host->abi_version != CD_ABI_VERSION) return -100;
  return host->register_element(&kGain);
}
