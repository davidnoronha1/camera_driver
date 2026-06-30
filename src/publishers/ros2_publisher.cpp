#ifdef CAMERA_DRIVER_WITH_ROS2

#include "camera_driver/publishers/ros2_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <gst/app/gstappsink.h>
#include <gst/video/video.h>
#include <sensor_msgs/msg/image.hpp>

namespace camera_driver {

namespace {
static std::atomic<int> g_ros2_id{0};
} // namespace

ROS2Publisher::ROS2Publisher(rclcpp::Node::SharedPtr node,
                             std::string topic,
                             std::string transport,
                             std::string camera_info_url,
                             std::string frame_id)
    : node_(node)
    , topic_(std::move(topic))
    , transport_(std::move(transport))
    , camera_info_url_(std::move(camera_info_url))
    , frame_id_(std::move(frame_id))
{
    name_       = "ros2_pub_" + std::to_string(g_ros2_id++);
    gst_string_ = fmt::format(
        "appsink name={} max-buffers=2 drop=true sync=false emit-signals=false", name_);
}

void ROS2Publisher::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("ROS2Publisher: appsink '" + name_ + "' not found");
    appsink_ = GST_APP_SINK(el);

    // Set up image_transport publisher
    image_transport::ImageTransport it(node_);
    image_pub_ = it.advertise(topic_ + "/image", 1);

    // Camera info publisher
    info_pub_ = node_->create_publisher<sensor_msgs::msg::CameraInfo>(
        topic_ + "/camera_info",
        rclcpp::SensorDataQoS());

    if (!camera_info_url_.empty()) {
        cim_ = std::make_shared<camera_info_manager::CameraInfoManager>(
            node_.get(), name_, camera_info_url_);
    }

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    LockFreeLogger::getInstance().info("ros2_pub",
        fmt::format("{} publishing to {}/image", name_, topic_));
}

void ROS2Publisher::bringdown(Pipeline* /*parent*/) {
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

GstFlowReturn ROS2Publisher::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<ROS2Publisher*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf  = gst_sample_get_buffer(sample);
    GstCaps*   caps = gst_sample_get_caps(sample);

    GstVideoInfo vinfo;
    if (!caps || !gst_video_info_from_caps(&vinfo, caps)) {
        gst_sample_unref(sample);
        return GST_FLOW_OK;
    }

    GstMapInfo map;
    if (!gst_buffer_map(buf, &map, GST_MAP_READ)) {
        gst_sample_unref(sample);
        return GST_FLOW_OK;
    }

    auto msg = std::make_unique<sensor_msgs::msg::Image>();
    msg->header.stamp    = self->node_->get_clock()->now();
    msg->header.frame_id = self->frame_id_;
    msg->width           = vinfo.width;
    msg->height          = vinfo.height;
    msg->step            = vinfo.stride[0];

    switch (GST_VIDEO_INFO_FORMAT(&vinfo)) {
        case GST_VIDEO_FORMAT_RGB:  msg->encoding = "rgb8"; break;
        case GST_VIDEO_FORMAT_BGR:  msg->encoding = "bgr8"; break;
        case GST_VIDEO_FORMAT_YUY2: msg->encoding = "yuv422"; break;
        default:                    msg->encoding = "rgb8"; break;
    }

    msg->data.assign(map.data, map.data + map.size);
    gst_buffer_unmap(buf, &map);
    gst_sample_unref(sample);

    self->image_pub_.publish(*msg);

    // Publish camera info if available
    if (self->cim_ && self->cim_->isCalibrated()) {
        auto info = self->cim_->getCameraInfo();
        info.header = msg->header;
        self->info_pub_->publish(info);
    }

    return GST_FLOW_OK;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(ROS2Publisher, [](const YAML::Node& cfg) {
    // ROS2Publisher requires a node — this factory can only be used when a node
    // is injected externally. For YAML use, the node must be set after creation.
    // Minimal stub for registry registration purposes:
    throw std::runtime_error(
        "ROS2Publisher cannot be created from YAML without providing an rclcpp::Node. "
        "Create it programmatically and add it to MuxElement::addBranch().");
    return std::shared_ptr<camera_driver::ROS2Publisher>{};
});

#endif // CAMERA_DRIVER_WITH_ROS2
