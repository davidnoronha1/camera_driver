#include "camera_driver/elements/undistort_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/gst/undistort_filter.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/metadata/camera_metadata.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <fstream>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_undistort_id{0};

double dcoef(const std::vector<double>& d, size_t i) {
    return i < d.size() ? d[i] : 0.0;
}

// Writes an nvdewarper config file for projection-type=3 ("Perspective to
// Perspective" — plain pinhole/radial-tangential undistort, not fisheye)
// from a CalibrationInfo. nvdewarper's distortion order is
// (k0,k1,k2 radial; k3,k4 tangential) = our D reordered from OpenCV/ROS
// plumb_bob order [k1,k2,p1,p2,k3] to [k1,k2,k3,p1,p2].
// Verified against a real nvdewarper build (see UndistortElement docs).
std::string writeDewarperConfig(const std::string& path, const CalibrationInfo& c) {
    const auto& K = c.K; // row-major 3x3: fx=K[0], cx=K[2], fy=K[4], cy=K[5]
    double k1 = dcoef(c.D, 0), k2 = dcoef(c.D, 1), p1 = dcoef(c.D, 2),
           p2 = dcoef(c.D, 3), k3 = dcoef(c.D, 4);

    std::ofstream out(path);
    if (!out) throw std::runtime_error("UndistortElement: cannot write config file " + path);
    out << fmt::format(
        "[property]\n"
        "output-width={}\n"
        "output-height={}\n"
        "num-batch-buffers=1\n"
        "[surface0]\n"
        "projection-type=3\n"
        "surface-index=0\n"
        "width={}\n"
        "height={}\n"
        "focal-length={};{}\n"
        "src-x0={}\n"
        "src-y0={}\n"
        "distortion={};{};{};{};{}\n",
        c.width, c.height, c.width, c.height, K[0], K[4], K[2], K[5],
        k1, k2, k3, p1, p2);
    return path;
}

} // namespace

UndistortElement::UndistortElement()
    : UnresolvedSegment("undistort_" + std::to_string(g_undistort_id++), {})
{
    registerUndistortFilter(); // idempotent; must happen before gst_parse_launch

    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        auto& log = LockFreeLogger::getInstance();

        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps = ctx.upstream_caps;
        seg.output_caps = ctx.upstream_caps;

        auto passthrough = [&](const std::string& reason) {
            seg.gst_string = "";
            log.info("undistort", fmt::format("{}: passthrough ({})", self->name_, reason));
            return seg;
        };

        auto metadata_yaml = ctx.pipeline ? ctx.pipeline->scratchpad()->get(Scratchpad::kCameraMetadataKey)
                                           : std::nullopt;
        if (!metadata_yaml) return passthrough("no camera-metadata in scratchpad");

        CameraMetadata metadata = CameraMetadata::fromYaml(*metadata_yaml);
        if (!metadata.calibration) return passthrough("scratchpad metadata has no calibration");

        if (ctx.hw.has_nvdewarper && ctx.hw.has_nvvidconv) {
            std::string config_path = fmt::format("/tmp/camera_driver_undistort_{}.cfg", self->name_);
            writeDewarperConfig(config_path, *metadata.calibration);

            seg.gst_string = fmt::format(
                "{} ! video/x-raw(memory:NVMM),format=RGBA ! nvdewarper config-file={}",
                ctx.hw.nvvidconv_name, config_path);
            seg.output_caps.format = PixelFormat::RGBA;
            seg.output_caps.is_nvmm = true;
            self->cpu_mode_ = false;

            log.info("undistort", fmt::format("{}: undistorting via nvdewarper ({})",
                self->name_, config_path));
            return seg;
        }

        // CPU fallback: a real GstVideoFilter element (camera_driver_undistort,
        // see gst/undistort_filter.hpp), not OpenCV's cameraundistort — that
        // pulls in libopencv-imgcodecs, which collides with nvjpegenc here.
        self->cpu_mode_ = true;
        self->calib_ = *metadata.calibration;

        seg.gst_string = fmt::format(
            "videoconvert ! video/x-raw,format=RGB ! {} name={}_filter",
            kUndistortFilterElementName, self->name_);
        seg.output_caps.format = PixelFormat::RGB;
        seg.output_caps.is_nvmm = false;

        log.warn("undistort", fmt::format(
            "{}: nvdewarper unavailable — undistorting on CPU (no GPU acceleration, "
            "expect a real fps cost at high resolutions)", self->name_));
        return seg;
    };

    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

void UndistortElement::setup(Pipeline* parent) {
    if (!cpu_mode_) return;

    GstElement* filter = parent->getGstElement(name_ + "_filter");
    if (!filter) {
        throw std::runtime_error("UndistortElement: '" + name_ + "_filter' not found");
    }
    setUndistortFilterCalibration(filter, calib_);
    gst_object_unref(filter);

    LockFreeLogger::getInstance().info("undistort", fmt::format("{} CPU undistort filter ready", name_));
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(UndistortElement, [](const YAML::Node& /*cfg*/) {
    return std::make_shared<camera_driver::UndistortElement>();
});
