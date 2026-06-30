#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <condition_variable>
#include <gst/app/gstappsink.h>
#include <mutex>
#include <opencv2/core.hpp>
#include <queue>
#include <string>
#include <vector>

namespace camera_driver {

// Wraps GStreamer appsink. API mirrors cv::VideoCapture: call read() to pull frames.
// Frames arrive from the GStreamer thread and are queued for the caller.
class CustomPublisher : public PipelineElement {
public:
    explicit CustomPublisher(size_t max_queue = 4);

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    // Block until a frame is available or the pipeline stops. Returns false on stop/EOS.
    bool read(cv::Mat& frame);
    bool read(std::vector<uint8_t>& raw, Caps& caps);

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override {
        return { PixelFormat::RGB, PixelFormat::BGR, PixelFormat::YUYV };
    }
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    struct Frame {
        std::vector<uint8_t> data;
        Caps                 caps;
    };

    size_t     max_queue_;
    std::string gst_string_;

    GstAppSink*             appsink_ = nullptr;
    std::mutex              mutex_;
    std::condition_variable cv_;
    std::queue<Frame>       queue_;
    bool                    eos_ = false;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
    static void onEos(GstAppSink* sink, gpointer data);
};

} // namespace camera_driver
