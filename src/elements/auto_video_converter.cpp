#include "camera_driver/elements/auto_video_converter.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_avc_id{0};

std::string buildConvString(PixelFormat target) {
    std::string s = "videoconvert";
    switch (target) {
        case PixelFormat::I420:  return s + " ! video/x-raw,format=I420";
        case PixelFormat::NV12:  return s + " ! video/x-raw,format=NV12";
        case PixelFormat::RGB:   return s + " ! video/x-raw,format=RGB";
        case PixelFormat::BGR:   return s + " ! video/x-raw,format=BGR";
        case PixelFormat::YUYV:  return s + " ! video/x-raw,format=YUY2";
        default:                 return s;
    }
}
} // namespace

AutoVideoConverterElement::AutoVideoConverterElement(PixelFormat target)
    : target_(target)
{
    name_       = "auto_conv_" + std::to_string(g_avc_id++);
    gst_string_ = buildConvString(target);
    output_.format = (target != PixelFormat::Unknown) ? target : PixelFormat::Unknown;
    output_.is_any = (target == PixelFormat::Unknown);
}

void AutoVideoConverterElement::setup(Pipeline* /*parent*/) {
    LockFreeLogger::getInstance().warn("auto_conv",
        fmt::format("[{}] Explicit conversion element active — adds latency. "
            "Consider using OptimizedConverter to avoid this.", name_));
}

Caps AutoVideoConverterElement::outputCapsFor(PixelFormat input) const {
    if (!output_.is_any) return output_;
    Caps c;
    c.format = (input != PixelFormat::Unknown) ? input : PixelFormat::YUYV;
    return c;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(AutoVideoConverterElement, [](const YAML::Node& cfg) {
    camera_driver::PixelFormat t = camera_driver::PixelFormat::Unknown;
    if (cfg["target"]) t = camera_driver::pixelFormatFromString(cfg["target"].as<std::string>());
    return std::make_shared<camera_driver::AutoVideoConverterElement>(t);
});
