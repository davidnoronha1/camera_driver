#pragma once

#ifdef CAMERA_DRIVER_WITH_ROS2

#include "../pipeline/pipeline_element.hpp"
#include <gst/app/gstappsink.h>
#include <image_transport/image_transport.hpp>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <string>
#include <camera_info_manager/camera_info_manager.hpp>

namespace camera_driver {

// Publishes frames via image_transport. Wraps an appsink.
// Prefers RGB input (sensor_msgs/Image uses RGB encoding by default).
// Optionally publishes camera_info from a camera_info_manager URL.
class ROS2Publisher : public PipelineElement {
public:
    ROS2Publisher(rclcpp::Node::SharedPtr node,
                  std::string topic,
                  std::string transport    = "raw",
                  std::string camera_info_url = "",
                  std::string frame_id     = "camera");

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override {
        return { PixelFormat::RGB };
    }
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    rclcpp::Node::SharedPtr node_;
    std::string topic_;
    std::string transport_;
    std::string camera_info_url_;
    std::string frame_id_;
    std::string gst_string_;

    GstAppSink* appsink_ = nullptr;
    image_transport::Publisher image_pub_;
    rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr info_pub_;
    std::shared_ptr<camera_info_manager::CameraInfoManager> cim_;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
};

} // namespace camera_driver

#endif // CAMERA_DRIVER_WITH_ROS2
