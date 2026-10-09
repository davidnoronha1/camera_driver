#include "camera_driver/elements/rtsp_src_element.hpp"
#include "camera_driver/element_registry.hpp"
#include <atomic>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_rtsp_src_id{0};
} // namespace

RTSPSourceElement::RTSPSourceElement(std::string url, int latency_ms, PixelFormat codec)
{
    name_ = "rtsp_src_" + std::to_string(g_rtsp_src_id++);

    switch (codec) {
        case PixelFormat::MJPEG:
            gst_string_ = fmt::format(
                "rtspsrc location={} latency={} ! rtpjpegdepay ! jpegparse",
                url, latency_ms);
            output_caps_.format = PixelFormat::MJPEG;
            break;
        case PixelFormat::H264:
        default:
            gst_string_ = fmt::format(
                "rtspsrc location={} latency={} ! rtph264depay ! h264parse",
                url, latency_ms);
            output_caps_.format = PixelFormat::H264;
            break;
    }
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(RTSPSourceElement, [](const YAML::Node& cfg) {
    if (!cfg["url"]) throw std::runtime_error("RTSPSourceElement: 'url' is required");
    std::string url      = cfg["url"].as<std::string>();
    int latency_ms       = cfg["latency_ms"].as<int>(100);
    std::string codec_s  = cfg["codec"].as<std::string>("h264");

    camera_driver::PixelFormat codec = camera_driver::PixelFormat::H264;
    if (codec_s == "mjpeg") codec = camera_driver::PixelFormat::MJPEG;

    return std::make_shared<camera_driver::RTSPSourceElement>(url, latency_ms, codec);
});
