#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipelines/yaml_pipeline.hpp"
// Force-link all element registrations
#include "camera_driver/elements/gst_element.hpp"
#include "camera_driver/elements/v4l2_src_element.hpp"
#include "camera_driver/elements/custom_src_element.hpp"
#include "camera_driver/elements/optimized_converter.hpp"
#include "camera_driver/elements/mux_element.hpp"
#include "camera_driver/elements/auto_video_converter.hpp"
#include "camera_driver/publishers/mjpeg_publisher.hpp"
#include "camera_driver/publishers/nv_unixfd_publisher.hpp"
#include "camera_driver/publishers/custom_publisher.hpp"
#include "camera_driver/publishers/display_publisher.hpp"

#include <csignal>
#include <gst/gst.h>
#include <iostream>
#include <memory>
#include <string>

static std::shared_ptr<camera_driver::Pipeline> g_pipeline;

static void signalHandler(int /*sig*/) {
    if (g_pipeline) g_pipeline->stop();
}

int main(int argc, char* argv[]) {
    gst_init(&argc, &argv);

    LockFreeLogger::getInstance().initialize(
        std::make_unique<ConsoleAndFileLogWriter>(), QueueMode::THREADED);

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <pipeline_config.yaml>\n";
        return 1;
    }

    std::signal(SIGINT,  signalHandler);
    std::signal(SIGTERM, signalHandler);

    try {
        g_pipeline = camera_driver::YAMLPipeline::fromFile(argv[1]);
        g_pipeline->build();
        g_pipeline->run();
    } catch (const std::exception& e) {
        LockFreeLogger::getInstance().error("main", std::string("Fatal: ") + e.what());
        LockFreeLogger::getInstance().shutdown();
        return 1;
    }

    LockFreeLogger::getInstance().shutdown();
    return 0;
}
