#include "camera_driver/elements/infer_server_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <fmt/format.h>
#include <gst/gst.h>

namespace camera_driver {

InferServerElement::InferServerElement(std::string config_file)
    : config_file_(std::move(config_file))
{
    name_ = "infer_server_" + config_file_;
}

void InferServerElement::setup(Pipeline* /*parent*/) {
    // Probe pad templates to infer supported input formats — nvinferserver
    // is only present on DeepStream hosts, so this degrades to a warning
    // (not a hard failure) elsewhere, same as GstElement.
    GstElementFactory* factory = gst_element_factory_find("nvinferserver");
    if (!factory) {
        LockFreeLogger::getInstance().warn("infer_server",
            fmt::format("{} nvinferserver not found in GStreamer registry — "
                "this element will fail to launch. Install DeepStream to enable it.", name_));
        return;
    }

    const GList* templates = gst_element_factory_get_static_pad_templates(factory);
    for (const GList* t = templates; t; t = t->next) {
        auto* tmpl = static_cast<GstStaticPadTemplate*>(t->data);
        if (tmpl->direction != GST_PAD_SINK) continue;

        GstCaps* caps = gst_static_caps_get(&tmpl->static_caps);
        if (!caps) continue;
        for (guint i = 0; i < gst_caps_get_size(caps); ++i) {
            GstStructure* s = gst_caps_get_structure(caps, i);
            const gchar* name = gst_structure_get_name(s);
            if (g_str_equal(name, "video/x-raw")) {
                const gchar* fmt = gst_structure_get_string(s, "format");
                if (fmt) {
                    if (g_str_equal(fmt, "NV12"))
                        inferred_inputs_.push_back(PixelFormat::NV12);
                    else if (g_str_equal(fmt, "RGBA"))
                        inferred_inputs_.push_back(PixelFormat::RGBA);
                }
            }
        }
        gst_caps_unref(caps);
    }
    gst_object_unref(factory);

    if (inferred_inputs_.empty()) {
        // nvinferserver's sink caps are usually memory:NVMM, which won't
        // show up as a plain video/x-raw structure above — fall back to its
        // documented native format.
        inferred_inputs_ = { PixelFormat::NV12_NVMM, PixelFormat::NV12 };
    }
}

std::vector<PixelFormat> InferServerElement::preferredInputFormats() const {
    return inferred_inputs_;
}

Caps InferServerElement::outputCapsFor(PixelFormat input) const {
    // nvinferserver attaches metadata but does not change the video format.
    Caps c;
    c.format = input;
    return c;
}

std::string InferServerElement::gstString() const {
    return fmt::format("nvinferserver config-file-path={}", config_file_);
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(InferServerElement, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::InferServerElement>(
        cfg["config_file"].as<std::string>());
});
