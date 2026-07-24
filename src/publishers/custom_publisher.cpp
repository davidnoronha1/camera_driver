#include "camera_driver/publishers/custom_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <gst/app/gstappsink.h>
#include <gst/video/video.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_cpub_id{0};
} // namespace

CustomPublisher::CustomPublisher(size_t max_queue)
    : max_queue_(max_queue)
{
    name_       = "custom_pub_" + std::to_string(g_cpub_id++);
    gst_string_ = fmt::format(
        "appsink name={} max-buffers={} drop=false sync=false emit-signals=false",
        name_, max_queue_);
}

void CustomPublisher::setCallback(RawCallback cb) { raw_callback_ = std::move(cb); }

void CustomPublisher::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("CustomPublisher: appsink '" + name_ + "' not found");
    appsink_ = GST_APP_SINK(el);

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    cbs.eos        = onEos;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    const char* mode = raw_callback_ ? "callback" : "pull";
    LockFreeLogger::getInstance().info("custom_pub", fmt::format("{} ready ({})", name_, mode));
}

void CustomPublisher::bringdown(Pipeline* /*parent*/) {
    {
        std::lock_guard<std::mutex> lk(mutex_);
        eos_ = true;
    }
    cv_.notify_all();
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

bool CustomPublisher::read(std::vector<uint8_t>& raw, Caps& caps) {
    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(lk, [this]{ return !queue_.empty() || eos_; });
    if (queue_.empty()) return false;

    auto f = std::move(queue_.front());
    queue_.pop();
    raw  = std::move(f.data);
    caps = f.caps;
    return true;
}

GstFlowReturn CustomPublisher::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<CustomPublisher*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf   = gst_sample_get_buffer(sample);
    GstCaps*   gcaps = gst_sample_get_caps(sample);

    Frame f;
    if (gcaps) {
        GstVideoInfo vinfo;
        if (gst_video_info_from_caps(&vinfo, gcaps)) {
            f.caps.width   = vinfo.width;
            f.caps.height  = vinfo.height;
            f.caps.fps_num = vinfo.fps_n;
            f.caps.fps_den = vinfo.fps_d;
            switch (GST_VIDEO_INFO_FORMAT(&vinfo)) {
                case GST_VIDEO_FORMAT_YUY2:  f.caps.format = PixelFormat::YUYV;    break;
                case GST_VIDEO_FORMAT_NV12:  f.caps.format = PixelFormat::NV12;    break;
                case GST_VIDEO_FORMAT_I420:  f.caps.format = PixelFormat::I420;    break;
                case GST_VIDEO_FORMAT_RGB:   f.caps.format = PixelFormat::RGB;     break;
                case GST_VIDEO_FORMAT_BGR:   f.caps.format = PixelFormat::BGR;     break;
                default:                     f.caps.format = PixelFormat::Unknown; break;
            }
        }
    }

    GstMapInfo map;
    if (gst_buffer_map(buf, &map, GST_MAP_READ)) {
        f.data.assign(map.data, map.data + map.size);
        gst_buffer_unmap(buf, &map);
    }

    gst_sample_unref(sample);

    if (self->raw_callback_) {
        self->raw_callback_(std::move(f.data), f.caps);
    } else {
        std::lock_guard<std::mutex> lk(self->mutex_);
        if (self->queue_.size() < self->max_queue_)
            self->queue_.push(std::move(f));
        // else: drop (bounded queue)
        self->cv_.notify_one();
    }
    return GST_FLOW_OK;
}

void CustomPublisher::onEos(GstAppSink* /*sink*/, gpointer data) {
    auto* self = static_cast<CustomPublisher*>(data);
    std::lock_guard<std::mutex> lk(self->mutex_);
    self->eos_ = true;
    self->cv_.notify_all();
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(CustomPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::CustomPublisher>(cfg["max_queue"].as<size_t>(4));
});
