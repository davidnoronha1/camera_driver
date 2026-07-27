#include "camera_driver/publishers/iceoryx_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include "camera_driver/publishers/iceoryx_frame.hpp"
#include <atomic>
#include <cstring>
#include <fmt/format.h>
#include <gst/video/video.h>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "iceoryx_posh/capro/service_description.hpp"
#include "iceoryx_posh/popo/publisher_options.hpp"
#include "iceoryx_posh/popo/untyped_publisher.hpp"
#include "iceoryx_posh/runtime/posh_runtime.hpp"

namespace camera_driver {

namespace {
static std::atomic<int> g_iox_pub_id{0};

void ensureRuntimeInitialized() {
    static std::once_flag once;
    std::call_once(once, [] {
        iox::runtime::PoshRuntime::initRuntime(
            iox::RuntimeName_t(iox::TruncateToCapacity, "camera_driver"));
    });
}

std::vector<std::string> splitTopic(const std::string& topic) {
    std::vector<std::string> parts;
    std::stringstream ss(topic);
    std::string part;
    while (std::getline(ss, part, '/')) parts.push_back(part);
    return parts;
}
} // namespace

IceOryxPublisher::IceOryxPublisher(std::string topic)
    : topic_(std::move(topic))
{
    name_       = "iceoryx_pub_" + std::to_string(g_iox_pub_id++);
    gst_string_ = fmt::format(
        "appsink name={} max-buffers=2 drop=true sync=false emit-signals=false",
        name_);
}

IceOryxPublisher::~IceOryxPublisher() = default;

void IceOryxPublisher::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("IceOryxPublisher: appsink '" + name_ + "' not found");
    appsink_ = GST_APP_SINK(el);

    auto parts = splitTopic(topic_);
    if (parts.size() != 3) {
        throw std::runtime_error(
            "IceOryxPublisher: topic '" + topic_ + "' must have 3 parts "
            "(Service/Instance/Event), e.g. 'Orbbec/Camera/Frame'");
    }

    ensureRuntimeInitialized();

    iox::popo::PublisherOptions opts;
    opts.historyCapacity = 1U;
    opts.nodeName = iox::NodeName_t(iox::TruncateToCapacity, name_.c_str());

    publisher_ = std::make_unique<iox::popo::UntypedPublisher>(
        iox::capro::ServiceDescription{
            iox::capro::IdString_t(iox::TruncateToCapacity, parts[0].c_str()),
            iox::capro::IdString_t(iox::TruncateToCapacity, parts[1].c_str()),
            iox::capro::IdString_t(iox::TruncateToCapacity, parts[2].c_str())},
        opts);

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    LockFreeLogger::getInstance().info("iceoryx_pub",
        fmt::format("{} publishing on {}", name_, topic_));

    if (auto yaml = parent->scratchpad()->get(Scratchpad::kCameraMetadataKey)) {
        iox::popo::PublisherOptions calib_opts;
        calib_opts.historyCapacity = 1U;
        std::string calib_node_name = name_ + "_calib";
        calib_opts.nodeName = iox::NodeName_t(iox::TruncateToCapacity, calib_node_name.c_str());

        calibration_publisher_ = std::make_unique<iox::popo::UntypedPublisher>(
            iox::capro::ServiceDescription{
                iox::capro::IdString_t(iox::TruncateToCapacity, parts[0].c_str()),
                iox::capro::IdString_t(iox::TruncateToCapacity, parts[1].c_str()),
                iox::capro::IdString_t(iox::TruncateToCapacity, "Calibration")},
            calib_opts);

        calibration_publisher_->loan(static_cast<uint32_t>(yaml->size()))
            .and_then([&](auto& user_payload) {
                std::memcpy(user_payload, yaml->data(), yaml->size());
                calibration_publisher_->publish(user_payload);
            })
            .or_else([&](auto& error) {
                LockFreeLogger::getInstance().error("iceoryx_pub",
                    fmt::format("{} calibration loan failed (err={})",
                        name_, static_cast<int>(error)));
            });

        LockFreeLogger::getInstance().info("iceoryx_pub",
            fmt::format("{} published calibration on {}/{}/Calibration", name_, parts[0], parts[1]));
    }
}

void IceOryxPublisher::bringdown(Pipeline* /*parent*/) {
    calibration_publisher_.reset();
    publisher_.reset();
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

GstFlowReturn IceOryxPublisher::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<IceOryxPublisher*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf   = gst_sample_get_buffer(sample);
    GstCaps*   gcaps = gst_sample_get_caps(sample);

    Caps caps;
    if (gcaps) {
        GstVideoInfo vinfo;
        if (gst_video_info_from_caps(&vinfo, gcaps)) {
            caps.width   = vinfo.width;
            caps.height  = vinfo.height;
            caps.fps_num = vinfo.fps_n;
            caps.fps_den = vinfo.fps_d;
            switch (GST_VIDEO_INFO_FORMAT(&vinfo)) {
                case GST_VIDEO_FORMAT_YUY2:  caps.format = PixelFormat::YUYV;    break;
                case GST_VIDEO_FORMAT_NV12:  caps.format = PixelFormat::NV12;    break;
                case GST_VIDEO_FORMAT_I420:  caps.format = PixelFormat::I420;    break;
                case GST_VIDEO_FORMAT_RGB:   caps.format = PixelFormat::RGB;     break;
                case GST_VIDEO_FORMAT_BGR:   caps.format = PixelFormat::BGR;     break;
                default:                     caps.format = PixelFormat::Unknown; break;
            }
        }
    }

    GstMapInfo map;
    if (gst_buffer_map(buf, &map, GST_MAP_READ)) {
        const uint64_t data_size = map.size;
        const uint64_t payload_size = kIceOryxFrameHeaderSize + data_size;
        self->publisher_->loan(static_cast<uint32_t>(payload_size))
            .and_then([&](auto& user_payload) {
                auto* frame = static_cast<IceOryxFrame*>(user_payload);
                frame->timestamp_ns =
                    GST_BUFFER_PTS_IS_VALID(buf) ? static_cast<uint64_t>(GST_BUFFER_PTS(buf)) : 0;
                frame->sequence_number = self->sequence_.fetch_add(1);
                frame->width        = static_cast<uint32_t>(caps.width);
                frame->height       = static_cast<uint32_t>(caps.height);
                frame->pixel_format = static_cast<uint32_t>(caps.format);
                frame->_pad         = 0U;
                frame->data_size    = data_size;
                std::memcpy(frame->data, map.data, data_size);
                self->publisher_->publish(user_payload);
            })
            .or_else([&](auto& error) {
                LockFreeLogger::getInstance().error("iceoryx_pub",
                    fmt::format("{} loan failed (err={}). Pool may be undersized.",
                        self->name_, static_cast<int>(error)));
            });
        gst_buffer_unmap(buf, &map);
    }

    gst_sample_unref(sample);
    return GST_FLOW_OK;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(IceOryxPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::IceOryxPublisher>(
        cfg["topic"].as<std::string>());
});
