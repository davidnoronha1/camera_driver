#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>

namespace camera_driver {

// GStreamer RTSP client source element.
// Emits: rtspsrc location=<url> latency=<ms> ! rtph264depay ! h264parse
//     or: rtspsrc location=<url> latency=<ms> ! rtpjpegdepay ! jpegparse
// Output caps are H264 (default) or MJPEG depending on codec parameter.
// rtspsrc has dynamic pads; gst_parse_launch handles them the same way gst-launch-1.0 does.
class RTSPSourceElement : public PipelineElement {
public:
    explicit RTSPSourceElement(std::string url,
                               int latency_ms = 100,
                               PixelFormat codec = PixelFormat::H264);

    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }
    Caps outputCapsFor(PixelFormat /*input*/) const override { return output_caps_; }

private:
    std::string gst_string_;
    Caps        output_caps_;
};

} // namespace camera_driver
