#include "camera_driver/publishers/display_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_disp_id{0};
} // namespace

DisplayPublisher::DisplayPublisher(std::string window_name)
    : UnresolvedSegment("display_pub_" + std::to_string(g_disp_id++), {}, {
        PixelFormat::BGR,
        PixelFormat::RGB,
        PixelFormat::YUYV,
        PixelFormat::MJPEG,
        PixelFormat::NV12,
        PixelFormat::I420,
        PixelFormat::RGBA,
    })
    , window_name_(std::move(window_name))
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        PixelFormat input = ctx.upstream_caps.format;
        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps = ctx.upstream_caps;
        seg.output_caps = ctx.upstream_caps;

        std::string decode_prefix = "";
        if (input == PixelFormat::MJPEG) {
            decode_prefix = "jpegdec ! ";
        }

        // Use autovideoconvert and fpsdisplaysink
        seg.gst_string = fmt::format("{}autovideoconvert ! fpsdisplaysink text-overlay=true sync=false", decode_prefix);

        return seg;
    };

    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(DisplayPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::DisplayPublisher>(
        cfg["window"].as<std::string>("camera_driver"));
});
