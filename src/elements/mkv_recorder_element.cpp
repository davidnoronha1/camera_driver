#include "camera_driver/elements/mkv_recorder_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_mkv_id{0};
} // namespace

MkvRecorderElement::MkvRecorderElement(std::string location)
    : UnresolvedSegment("mkv_recorder_" + std::to_string(g_mkv_id++), {}, {
          PixelFormat::H264,
          PixelFormat::I420,
          PixelFormat::NV12,
          PixelFormat::RGB,
          PixelFormat::BGR,
          PixelFormat::YUYV,
      })
    , location_(std::move(location))
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        PixelFormat input = ctx.upstream_caps.format;
        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps = ctx.upstream_caps;
        seg.output_caps = ctx.upstream_caps;

        // H264(_NVMM) arrives already encoded — just parse it for the muxer.
        // Anything else is raw video; matroskamux accepts it directly once
        // it's in system memory, hence the plain videoconvert.
        std::string prefix = (input == PixelFormat::H264 || input == PixelFormat::H264_NVMM)
            ? "h264parse ! "
            : "videoconvert ! ";

        seg.gst_string = fmt::format(
            "{}matroskamux name={}_mux ! filesink name={}_sink location={}",
            prefix, self->name_, self->name_, self->location_);
        return seg;
    };
    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

void MkvRecorderElement::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_ + "_mux");
    if (!el) throw std::runtime_error("MkvRecorderElement: matroskamux '" + name_ + "_mux' not found");
    mux_ = el;
    if (pending_metadata_) applyMetadata(*pending_metadata_);

    LockFreeLogger::getInstance().info("mkv_recorder",
        fmt::format("{} recording to {}", name_, location_));
}

void MkvRecorderElement::bringdown(Pipeline* /*parent*/) {
    if (mux_) { gst_object_unref(mux_); mux_ = nullptr; }
}

void MkvRecorderElement::setMetadata(const CameraMetadata& metadata) {
    pending_metadata_ = metadata;
    if (mux_) applyMetadata(metadata);
}

void MkvRecorderElement::applyMetadata(const CameraMetadata& metadata) {
    // GST_TAG_COMMENT, not GST_TAG_EXTENDED_COMMENT: matroskamux silently
    // drops the latter (verified empirically) but writes/round-trips the
    // former as the file's global COMMENT tag.
    std::string tag_value = "camera-metadata=" + metadata.toYaml();
    GstTagList* tags = gst_tag_list_new(GST_TAG_COMMENT, tag_value.c_str(), nullptr);
    gst_tag_setter_merge_tags(GST_TAG_SETTER(mux_), tags, GST_TAG_MERGE_REPLACE);
    gst_tag_list_unref(tags);
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(MkvRecorderElement, [](const YAML::Node& cfg) {
    auto element = std::make_shared<camera_driver::MkvRecorderElement>(
        cfg["location"].as<std::string>());

    camera_driver::CameraMetadata metadata;
    bool have_metadata = false;
    if (cfg["metadata_file"]) {
        std::ifstream in(cfg["metadata_file"].as<std::string>());
        std::stringstream ss;
        ss << in.rdbuf();
        metadata = camera_driver::CameraMetadata::fromYaml(ss.str());
        have_metadata = true;
    } else if (cfg["calibration_file"]) {
        metadata.calibration = camera_driver::CameraMetadata::calibrationFromYamlFile(
            cfg["calibration_file"].as<std::string>());
        have_metadata = true;
    }
    if (have_metadata) element->setMetadata(metadata);
    return element;
});
