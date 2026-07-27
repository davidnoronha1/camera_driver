#include "camera_driver/elements/auto_video_converter.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_avc_id{0};

bool isBayer(PixelFormat f) {
    return f == PixelFormat::BayerRGGB || f == PixelFormat::BayerBGGR ||
           f == PixelFormat::BayerGRBG || f == PixelFormat::BayerGBRG;
}

std::string targetCapsSuffix(PixelFormat target) {
    switch (target) {
        case PixelFormat::I420: return " ! video/x-raw,format=I420";
        case PixelFormat::NV12: return " ! video/x-raw,format=NV12";
        case PixelFormat::RGB:  return " ! video/x-raw,format=RGB";
        case PixelFormat::BGR:  return " ! video/x-raw,format=BGR";
        case PixelFormat::YUYV: return " ! video/x-raw,format=YUY2";
        default:                return "";
    }
}
} // namespace

AutoVideoConverterElement::AutoVideoConverterElement(PixelFormat target)
    : UnresolvedSegment("auto_conv_" + std::to_string(g_avc_id++), {})
    , target_(target)
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps = ctx.upstream_caps;

        // bayer2rgb must run before videoconvert — videoconvert has no
        // debayering capability of its own.
        std::string prefix = isBayer(ctx.upstream_caps.format) ? "bayer2rgb ! " : "";
        seg.gst_string = prefix + "videoconvert" + targetCapsSuffix(self->target_);

        if (self->target_ != PixelFormat::Unknown) {
            seg.output_caps.format = self->target_;
        } else {
            seg.output_caps.format = (ctx.upstream_caps.format != PixelFormat::Unknown)
                ? ctx.upstream_caps.format : PixelFormat::YUYV;
        }
        return seg;
    };
    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver));
}

void AutoVideoConverterElement::setup(Pipeline* /*parent*/) {
    LockFreeLogger::getInstance().warn("auto_conv",
        fmt::format("[{}] Explicit conversion element active — adds latency. "
            "Consider using OptimizedConverter to avoid this.", name_));
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(AutoVideoConverterElement, [](const YAML::Node& cfg) {
    camera_driver::PixelFormat t = camera_driver::PixelFormat::Unknown;
    if (cfg["target"]) t = camera_driver::pixelFormatFromString(cfg["target"].as<std::string>());
    return std::make_shared<camera_driver::AutoVideoConverterElement>(t);
});
