#include "camera_driver/publishers/nv_unixfd_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/hw_detect.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_nvfd_id{0};
} // namespace

NVUnixFDPublisher::NVUnixFDPublisher(std::string socket_path)
    : socket_path_(std::move(socket_path))
{
    name_      = "nv_unixfd_" + std::to_string(g_nvfd_id++);
    available_ = hw::probe().has_nvunixfdsink;

    if (available_) {
        gst_string_ = fmt::format("nvunixfdsink socket-path={} sync=false", socket_path_);
    } else {
        gst_string_ = "fakesink sync=false";
    }
}

std::vector<PixelFormat> NVUnixFDPublisher::preferredInputFormats() const {
    if (available_) return { PixelFormat::NV12_NVMM, PixelFormat::NV12 };
    return {};
}

void NVUnixFDPublisher::setup(Pipeline* /*parent*/) {
    auto& log = LockFreeLogger::getInstance();
    if (available_) {
        log.info("nv_unixfd", fmt::format("{} nvunixfdsink → {}", name_, socket_path_));
    } else {
        log.warn("nv_unixfd",
            fmt::format("{} nvunixfdsink not available — this branch is a no-op (fakesink). "
                "Install NVIDIA GStreamer plugins to enable GPU zero-copy output.", name_));
    }
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(NVUnixFDPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::NVUnixFDPublisher>(
        cfg["socket"].as<std::string>("/tmp/camera_nv.sock"));
});
