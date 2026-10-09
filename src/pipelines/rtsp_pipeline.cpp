#ifdef CAMERA_DRIVER_WITH_RTSP

#include "camera_driver/pipelines/rtsp_pipeline.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/segment.hpp"
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

RTSPPipeline::RTSPPipeline(int port, std::string mount_point)
    : port_(port), mount_point_(std::move(mount_point))
{}

RTSPPipeline::~RTSPPipeline() {
    if (rtsp_server_)   { gst_object_unref(rtsp_server_);   rtsp_server_   = nullptr; }
    if (mount_points_)  { gst_object_unref(mount_points_);  mount_points_  = nullptr; }
}

std::string RTSPPipeline::assemblePipeline() {
    // Base chain (elements before the RTSP tail)
    std::string base;
    for (const auto& s : resolved_linear_) {
        if (s.gst_string.empty()) continue;
        if (!base.empty()) base += " ! ";
        base += s.gst_string;
    }

    // Validate: last resolved segment must output H264
    if (!resolved_linear_.empty()) {
        const auto& last = resolved_linear_.back();
        if (last.output_caps.format != PixelFormat::H264 &&
            last.output_caps.format != PixelFormat::H264_NVMM &&
            !last.output_caps.is_any)
        {
            throw std::runtime_error(fmt::format(
                "RTSPPipeline: last element '{}' outputs {} but H264 is required. "
                "Add OptimizedConverter({{PixelFormat::H264}}) before RTSPPipeline.",
                last.name, pixelFormatName(last.output_caps.format)));
        }
    }

    // Append RTP payload for RTSP
    base += " ! h264parse ! rtph264pay name=pay0 pt=96 config-interval=1";
    return base;
}

void RTSPPipeline::build() {
    // Run the parent resolution and validation (sets resolved_linear_)
    hw_ = hw::probe();
    resolveSegments();
    validateCaps();
    gst_string_ = assemblePipeline();

    LockFreeLogger::getInstance().info("rtsp_pipeline", "GST launch string: " + gst_string_);

    // Don't call gst_parse_launch here — the RTSP server creates the pipeline from the string.
    // Just call setup() on each element with a null pipeline (they have nothing to retrieve yet).
    // Elements that need GstElement* references (appsink/appsrc) are set up in run().
}

void RTSPPipeline::run() {
    if (!glib_loop_) glib_loop_ = g_main_loop_new(nullptr, FALSE);

    rtsp_server_ = gst_rtsp_server_new();
    gst_rtsp_server_set_service(rtsp_server_, std::to_string(port_).c_str());

    mount_points_ = gst_rtsp_server_get_mount_points(rtsp_server_);

    GstRTSPMediaFactory* factory = gst_rtsp_media_factory_new();
    gst_rtsp_media_factory_set_launch(factory, fmt::format("( {} )", gst_string_).c_str());
    gst_rtsp_media_factory_set_shared(factory, TRUE);

    gst_rtsp_mount_points_add_factory(mount_points_, mount_point_.c_str(), factory);
    gst_object_unref(mount_points_); mount_points_ = nullptr;

    GSource* src = gst_rtsp_server_create_source(rtsp_server_, nullptr, nullptr);
    g_source_attach(src, g_main_loop_get_context(glib_loop_));
    g_source_unref(src);

    LockFreeLogger::getInstance().info("rtsp_pipeline",
        fmt::format("RTSP server on rtsp://0.0.0.0:{}{}", port_, mount_point_));

    g_main_loop_run(glib_loop_);

    LockFreeLogger::getInstance().info("rtsp_pipeline", "RTSP server stopped");
}

} // namespace camera_driver

#endif // CAMERA_DRIVER_WITH_RTSP
