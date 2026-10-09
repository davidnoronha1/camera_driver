#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <atomic>
#include <gst/app/gstappsrc.h>
#include <string>

namespace camera_driver {

// Wraps GStreamer appsrc: create, write frames.
// Connects to the pipeline via a named appsrc element.
// Deliberately takes raw buffers rather than cv::Mat: this process must not
// link OpenCV (and therefore libjpeg) alongside nvjpegenc, since its ABI
// collides with libnvds_lljpeg.so and aborts the process. Callers that want
// cv::Mat convenience should wrap this from their own process/binary.
class CustomSrcElement : public PipelineElement {
public:
    CustomSrcElement(int width, int height, PixelFormat fmt, int fps = 30);

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    // Push a frame (thread-safe). Returns false if the pipeline is stopping.
    bool write(const uint8_t* data, size_t size_bytes);

    std::string gstString() const override { return gst_string_; }
    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }
    Caps outputCapsFor(PixelFormat /*input*/) const override { return output_caps_; }

private:
    int         width_, height_, fps_;
    PixelFormat format_;
    Caps        output_caps_;
    std::string gst_string_;

    GstAppSrc*         appsrc_ = nullptr;
    std::atomic<bool>  running_{false};
    uint64_t           frame_count_ = 0;
};

} // namespace camera_driver
