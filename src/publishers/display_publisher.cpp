#include "camera_driver/publishers/display_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/hw_detect.hpp"
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

        // Prefer nveglglessink over autovideosink's default ranking (which
        // picks kmssink) only when it's actually available. kmssink does
        // direct DRM mode-setting, which requires being DRM master for the
        // GPU — but the host's X server already holds DRM master while it's
        // driving the desktop, so kmssink's drmModeSetPlane is always refused
        // (EPERM) whenever a real X session is running on the same card.
        // nveglglessink instead presents through the X server via EGL/GLX
        // (like any normal GL application window), so it only needs render-node
        // access, not DRM master. Where nveglglessink isn't present (non-NVIDIA
        // hardware), fall back to plain autovideosink.
        // text-overlay is off because this image's GStreamer wasn't built with
        // pango (textoverlay unavailable).
        std::string video_sink = hw::gstHasElement("nveglglessink") ? " video-sink=nveglglessink" : "";
        seg.gst_string = fmt::format(
            "{}autovideoconvert ! fpsdisplaysink{} text-overlay=false sync=false",
            decode_prefix, video_sink);

        return seg;
    };

    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(DisplayPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::DisplayPublisher>(
        cfg["window"].as<std::string>("camera_driver"));
});
