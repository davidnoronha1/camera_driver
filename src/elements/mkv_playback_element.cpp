#include "camera_driver/elements/mkv_playback_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/hw_detect.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/metadata/metadata_io.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_mkv_playback_id{0};
} // namespace

MkvPlaybackElement::MkvPlaybackElement(std::string location)
    : UnresolvedSegment("mkv_playback_" + std::to_string(g_mkv_playback_id++), {})
    , location_(std::move(location))
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        auto& log = LockFreeLogger::getInstance();

        // Read and publish calibration/pose to the scratchpad here (not in
        // setup()) so it's available before any later element's resolver
        // runs — see the header comment for why.
        if (ctx.pipeline) {
            try {
                auto metadata = metadata_io::readMetadataFromFile(self->location_);
                if (metadata) {
                    ctx.pipeline->scratchpad()->set(Scratchpad::kCameraMetadataKey, metadata->toYaml());
                    log.info("mkv_playback", fmt::format(
                        "{} published camera-metadata to scratchpad (calibration={}, pose={})",
                        self->name_, metadata->calibration.has_value(), metadata->pose.has_value()));
                } else {
                    log.warn("mkv_playback", fmt::format(
                        "{} no camera-metadata found in {}", self->name_, self->location_));
                }
            } catch (const std::exception& e) {
                log.warn("mkv_playback", fmt::format(
                    "{} failed to read metadata from {}: {} — continuing without it",
                    self->name_, self->location_, e.what()));
            }
        }

        // nvv4l2decoder is DeepStream's cross-platform (Jetson + dGPU)
        // hardware decoder and outputs NVMM directly — required for
        // UndistortElement's nvdewarper path. Prefer it; fall back to
        // vanilla NVDEC, then software.
        std::string decoder;
        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps.is_any = true;

        if (ctx.hw.has_nvv4l2h264dec) {
            decoder = "nvv4l2decoder";
            seg.output_caps.format = PixelFormat::NV12_NVMM;
            seg.output_caps.is_nvmm = true;
        } else if (ctx.hw.has_nvh264dec) {
            decoder = "nvh264dec";
            seg.output_caps.format = PixelFormat::NV12;
        } else if (ctx.hw.has_avdec_h264) {
            decoder = "avdec_h264";
            seg.output_caps.format = PixelFormat::I420;
        } else {
            throw std::runtime_error(
                "MkvPlaybackElement: no H264 decoder available (install NVIDIA "
                "DeepStream decode plugins, or gstreamer1.0-libav for avdec_h264)");
        }

        seg.gst_string = fmt::format(
            "filesrc location={} ! matroskademux name={}_demux ! h264parse ! {}",
            self->location_, self->name_, decoder);

        log.info("mkv_playback", fmt::format("{} playing back {} via {}",
            self->name_, self->location_, decoder));
        return seg;
    };

    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(MkvPlaybackElement, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::MkvPlaybackElement>(
        cfg["location"].as<std::string>());
});
