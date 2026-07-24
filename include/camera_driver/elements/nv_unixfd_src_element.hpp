#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>

namespace camera_driver {

// NvUnixFdSrcElement — uses nvunixfdsrc to receive GPU buffers over a Unix
// socket from a peer process writing with nvunixfdsink (see NVUnixFDPublisher).
// The socket carries opaque frames; the caps of what's sent are not
// negotiated through gst_parse_launch (the pad template is ANY), so the
// expected format must be known and passed in here to match the sender.
//
// When nvunixfdsrc is available: adds the element, connecting to socket_path.
// When nvunixfdsrc is NOT available: logs a warning and falls back to a
// videotestsrc producing plain (non-NVMM) raw frames, so the rest of the
// pipeline still gets something to negotiate against.
class NvUnixFdSrcElement : public PipelineElement {
public:
    explicit NvUnixFdSrcElement(std::string socket_path,
                                 PixelFormat format = PixelFormat::NV12_NVMM,
                                 int width = 1280,
                                 int height = 720,
                                 int connection_attempts = -1);

    void setup(Pipeline* parent) override;

    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }
    Caps outputCapsFor(PixelFormat /*input*/) const override { return output_caps_; }

private:
    std::string socket_path_;
    std::string gst_string_;
    Caps        output_caps_;
    bool        available_ = false;
};

} // namespace camera_driver
