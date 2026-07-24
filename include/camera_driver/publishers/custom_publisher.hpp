#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <condition_variable>
#include <functional>
#include <gst/app/gstappsink.h>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace camera_driver {

// Wraps GStreamer appsink. Two usage modes:
//   Pull mode  — call read() to block until a frame arrives.
//   Callback mode — call setCallback() before Pipeline::build(); frames are delivered
//                   on the GStreamer thread, queue and read() are not used.
// Deliberately raw-buffer only (no cv::Mat): this process must not link
// OpenCV (and therefore libjpeg) alongside nvjpegenc, since its ABI collides
// with libnvds_lljpeg.so and aborts the process. Callers that want cv::Mat
// convenience should wrap this from their own process/binary.
class CustomPublisher : public PipelineElement {
public:
    // Callback signature — set at most one before build().
    using RawCallback = std::function<void(std::vector<uint8_t>, Caps)>;

    explicit CustomPublisher(size_t max_queue = 4);

    // Set a callback instead of using the pull API. Must be called before build().
    void setCallback(RawCallback cb);

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    // Pull API — block until a frame is available or the pipeline stops.
    // Returns false on stop/EOS. Do not use when a callback is set.
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

    size_t      max_queue_;
    std::string gst_string_;

    RawCallback raw_callback_;

    GstAppSink*             appsink_ = nullptr;
    std::mutex              mutex_;
    std::condition_variable cv_;
    std::queue<Frame>       queue_;
    bool                    eos_ = false;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
    static void onEos(GstAppSink* sink, gpointer data);
};

} // namespace camera_driver
