#include "camera_driver/elements/nv_unixfd_src_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/hw_detect.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_nvfd_src_id{0};
} // namespace

NvUnixFdSrcElement::NvUnixFdSrcElement(std::string socket_path,
                                       PixelFormat format, int width,
                                       int height, int connection_attempts)
    : socket_path_(std::move(socket_path)) {
  name_ = "nv_unixfd_src_" + std::to_string(g_nvfd_src_id++);
  available_ = hw::probe().has_nvunixfdsrc;

  if (available_) {
    gst_string_ = fmt::format(
        "nvunixfdsrc socket-path={} connection-attempts={} do-timestamp=true",
        socket_path_, connection_attempts);
    output_caps_.format = format;
    output_caps_.width = width;
    output_caps_.height = height;
    output_caps_.is_nvmm =
        (format == PixelFormat::NV12_NVMM || format == PixelFormat::H264_NVMM);
  } else {
    // No-op stand-in: nvunixfdsrc isn't installed, so produce a plain raw
    // test pattern instead of stalling the pipeline. Not GPU memory and
    // not connected to any peer process — see setup() warning.
    gst_string_ =
        fmt::format("videotestsrc is-live=true pattern=smpte ! "
                    "video/x-raw,format=NV12,width={},height={},framerate=30/1",
                    width, height);
    output_caps_.format = PixelFormat::NV12;
    output_caps_.width = width;
    output_caps_.height = height;
    output_caps_.is_nvmm = false;
  }
}

void NvUnixFdSrcElement::setup(Pipeline * /*parent*/) {
  auto &log = LockFreeLogger::getInstance();
  if (available_) {
    log.info("nv_unixfd_src",
             fmt::format("{} nvunixfdsrc ← {}", name_, socket_path_));
  } else {
    log.warn(
        "nv_unixfd_src",
        fmt::format(
            "{} nvunixfdsrc not available — this branch is a no-op "
            "(videotestsrc, no real data). Install NVIDIA GStreamer plugins "
            "to receive GPU buffers from a peer process.",
            name_));
  }
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(NvUnixFdSrcElement, [](const YAML::Node &cfg) {
  if (!cfg["socket"])
    throw std::runtime_error("NvUnixFdSrcElement: 'socket' is required");
  std::string socket_path = cfg["socket"].as<std::string>();
  std::string format_s = cfg["format"].as<std::string>("nv12_nvmm");
  int width = cfg["width"].as<int>(1280);
  int height = cfg["height"].as<int>(720);
  int connection_attempts = cfg["connection_attempts"].as<int>(-1);

  camera_driver::PixelFormat format = camera_driver::PixelFormat::NV12_NVMM;
  if (format_s == "nv12")
    format = camera_driver::PixelFormat::NV12;
  else if (format_s == "h264_nvmm")
    format = camera_driver::PixelFormat::H264_NVMM;
  else if (format_s == "h264")
    format = camera_driver::PixelFormat::H264;

  return std::make_shared<camera_driver::NvUnixFdSrcElement>(
      socket_path, format, width, height, connection_attempts);
});
