#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <atomic>
#include <condition_variable>
#include <gst/app/gstappsink.h>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

namespace camera_driver {

// HTTP multipart MJPEG streaming server. Wraps a GStreamer appsink.
// Requires MJPEG input; upstream elements (e.g. OptimizedConverter) must
// encode to JPEG (jpegenc/nvjpegenc) before this publisher.
//
// GET /calibration serves the pipeline-level scratchpad's camera metadata
// (see Scratchpad::kCameraMetadataKey) as YAML, 404 if none was provided.
class MJPEGPublisher : public PipelineElement {
public:
    explicit MJPEGPublisher(int port = 8080, int jpeg_quality = 85);
    ~MJPEGPublisher() override;

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }
    std::vector<PixelFormat> preferredInputFormats() const override;
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

    // Called from the appsink new-sample callback.
    void pushJpegFrame(const uint8_t* data, size_t size);

private:
    int  port_;
    int  jpeg_quality_;
    std::string gst_string_;
    PixelFormat received_format_ = PixelFormat::Unknown;

    // Camera calibration/pose YAML (see Scratchpad::kCameraMetadataKey),
    // read once at setup() and served at GET /calibration. Empty if the
    // pipeline-level scratchpad had none.
    std::string calibration_yaml_;

    GstAppSink* appsink_ = nullptr;

    // HTTP server state
    int              listen_fd_ = -1;
    std::thread      listen_thread_;
    std::atomic<bool> running_{false};

    // Latest JPEG frame
    std::mutex               frame_mutex_;
    std::condition_variable  frame_cv_;
    std::vector<uint8_t>     latest_frame_;
    uint64_t                 frame_id_ = 0;

    void listenLoop();
    void serveClient(int client_fd);

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
};

} // namespace camera_driver
