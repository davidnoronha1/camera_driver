#include "camera_driver/elements/custom_src_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <gst/app/gstappsrc.h>
#include <opencv2/imgproc.hpp>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_src_id{0};
} // namespace

CustomSrcElement::CustomSrcElement(int width, int height, PixelFormat fmt, int fps)
    : width_(width), height_(height), fps_(fps), format_(fmt)
{
    name_ = "custom_src_" + std::to_string(g_src_id++);

    output_caps_.format  = fmt;
    output_caps_.width   = width;
    output_caps_.height  = height;
    output_caps_.fps_num = fps;
    output_caps_.fps_den = 1;

    gst_string_ = fmt::format(
        "appsrc name={} caps={} format=time is-live=true block=true do-timestamp=true",
        name_, output_caps_.toGstCapsString());
}

void CustomSrcElement::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("CustomSrcElement: appsrc '" + name_ + "' not found");
    appsrc_ = GST_APP_SRC(el);
    gst_app_src_set_stream_type(appsrc_, GST_APP_STREAM_TYPE_STREAM);
    running_ = true;
    LockFreeLogger::getInstance().info("custom_src", fmt::format("{} ready ({}x{}@{})",
        name_, width_, height_, fps_));
}

void CustomSrcElement::bringdown(Pipeline* /*parent*/) {
    running_ = false;
    if (appsrc_) {
        gst_app_src_end_of_stream(appsrc_);
        gst_object_unref(appsrc_);
        appsrc_ = nullptr;
    }
}

bool CustomSrcElement::write(const cv::Mat& frame) {
    if (!appsrc_ || !running_) return false;

    // Convert to the declared format if needed
    cv::Mat converted;
    if (format_ == PixelFormat::RGB && frame.channels() == 3 &&
        frame.type() == CV_8UC3) {
        // OpenCV stores as BGR by default
        cv::cvtColor(frame, converted, cv::COLOR_BGR2RGB);
    } else {
        converted = frame;
    }

    size_t size = static_cast<size_t>(converted.total() * converted.elemSize());
    return write(converted.data, size);
}

bool CustomSrcElement::write(const uint8_t* data, size_t size_bytes) {
    if (!appsrc_ || !running_) return false;

    GstBuffer* buf = gst_buffer_new_allocate(nullptr, size_bytes, nullptr);
    if (!buf) return false;

    GstMapInfo map;
    if (gst_buffer_map(buf, &map, GST_MAP_WRITE)) {
        std::memcpy(map.data, data, size_bytes);
        gst_buffer_unmap(buf, &map);
    }

    GstFlowReturn ret = gst_app_src_push_buffer(appsrc_, buf);
    ++frame_count_;
    return ret == GST_FLOW_OK;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(CustomSrcElement, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::CustomSrcElement>(
        cfg["width"].as<int>(640),
        cfg["height"].as<int>(480),
        camera_driver::pixelFormatFromString(cfg["format"].as<std::string>("RGB")),
        cfg["fps"].as<int>(30));
});
