#include "camera_driver/elements/gst_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <fmt/format.h>
#include <gst/gst.h>

namespace camera_driver {

GstElement::GstElement(std::string element_name, std::string properties)
    : element_name_(std::move(element_name))
    , properties_(std::move(properties))
{
    name_ = element_name_;
    inferred_output_.is_any = true;
}

void GstElement::setup(Pipeline* /*parent*/) {
    // Probe pad templates to infer supported formats
    GstElementFactory* factory = gst_element_factory_find(element_name_.c_str());
    if (!factory) {
        LockFreeLogger::getInstance().warn("gst_element",
            fmt::format("Element '{}' not found in registry", element_name_));
        return;
    }

    const GList* templates = gst_element_factory_get_static_pad_templates(factory);
    for (const GList* t = templates; t; t = t->next) {
        auto* tmpl = static_cast<GstStaticPadTemplate*>(t->data);
        if (tmpl->direction == GST_PAD_SINK) {
            // Parse sink caps to infer preferred inputs
            GstCaps* caps = gst_static_caps_get(&tmpl->static_caps);
            if (caps) {
                for (guint i = 0; i < gst_caps_get_size(caps); ++i) {
                    GstStructure* s = gst_caps_get_structure(caps, i);
                    const gchar* name = gst_structure_get_name(s);
                    if (g_str_equal(name, "video/x-raw")) {
                        const gchar* fmt = gst_structure_get_string(s, "format");
                        if (fmt) {
                            if (g_str_equal(fmt, "YUY2") || g_str_equal(fmt, "YUYV"))
                                inferred_inputs_.push_back(PixelFormat::YUYV);
                            else if (g_str_equal(fmt, "NV12"))
                                inferred_inputs_.push_back(PixelFormat::NV12);
                            else if (g_str_equal(fmt, "I420"))
                                inferred_inputs_.push_back(PixelFormat::I420);
                            else if (g_str_equal(fmt, "RGB"))
                                inferred_inputs_.push_back(PixelFormat::RGB);
                            else if (g_str_equal(fmt, "BGR"))
                                inferred_inputs_.push_back(PixelFormat::BGR);
                        }
                    } else if (g_str_equal(name, "image/jpeg")) {
                        inferred_inputs_.push_back(PixelFormat::MJPEG);
                    } else if (g_str_equal(name, "video/x-h264")) {
                        inferred_inputs_.push_back(PixelFormat::H264);
                    }
                }
                gst_caps_unref(caps);
            }
        }
    }
    gst_object_unref(factory);
}

std::vector<PixelFormat> GstElement::preferredInputFormats() const {
    return inferred_inputs_;
}

Caps GstElement::outputCapsFor(PixelFormat /*input*/) const {
    return inferred_output_; // any — GStreamer negotiates
}

std::string GstElement::gstString() const {
    if (properties_.empty()) return element_name_;
    return element_name_ + " " + properties_;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(GstElement, [](const YAML::Node& cfg) {
    std::string element = cfg["element"].as<std::string>();
    std::string props   = cfg["properties"] ? cfg["properties"].as<std::string>() : "";
    return std::make_shared<camera_driver::GstElement>(element, props);
});
