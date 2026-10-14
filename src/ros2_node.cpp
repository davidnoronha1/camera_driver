#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipelines/yaml_pipeline.hpp"
#include "camera_driver/publishers/ros2_publisher.hpp"
#include <gst/gst.h>
#include <rclcpp/rclcpp.hpp>
#include <memory>
#include <stdexcept>
#include <string>

int main(int argc, char* argv[]) {
    rclcpp::init(argc, argv);
    gst_init(nullptr, nullptr);
    LockFreeLogger::getInstance().initialize(
        std::make_unique<ConsoleAndFileLogWriter>(), QueueMode::THREADED);
    int result = 0;
    try {
        auto node = std::make_shared<rclcpp::Node>("camera_driver");
        const auto config = node->declare_parameter<std::string>("pipeline_config", "");
        if (config.empty()) {
            throw std::runtime_error("Set pipeline_config to a pipeline YAML file");
        }
        camera_driver::ElementRegistry::instance().registerElement("ROS2Publisher",
            [weak_node = std::weak_ptr<rclcpp::Node>(node)](const YAML::Node& cfg) {
                auto owner = weak_node.lock();
                if (!owner) throw std::runtime_error("ROS node has expired");
                return std::make_shared<camera_driver::ROS2Publisher>(
                    owner, cfg["topic"].as<std::string>(),
                    cfg["transport"].as<std::string>("raw"),
                    cfg["camera_info_url"].as<std::string>(""),
                    cfg["frame_id"].as<std::string>("camera"));
            });
        auto pipeline = camera_driver::YAMLPipeline::fromFile(config);
        pipeline->build();
        rclcpp::executors::SingleThreadedExecutor executor;
        executor.add_node(node);
        pipeline->start();
        rclcpp::WallRate rate(100);
        while (rclcpp::ok() && pipeline->tick()) {
            executor.spin_some();
            rate.sleep();
        }
        pipeline->stop();
        pipeline->tick();
    } catch (const std::exception& error) {
        RCLCPP_ERROR(rclcpp::get_logger("camera_driver"), "%s", error.what());
        result = 1;
    }
    // Destroy the pipeline (including streaming callbacks) before logger shutdown.
    LockFreeLogger::getInstance().shutdown();
    if (rclcpp::ok()) rclcpp::shutdown();
    return result;
}
