#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>
#include <vector>

namespace camera_driver {

// NVUnixFDPublisher — uses nvunixfdsink to share GPU buffers via a Unix socket.
// When nvunixfdsink is available: adds the element, strongly prefers NV12_NVMM input
// to keep the entire pipeline in GPU memory (zero copies).
// When nvunixfdsink is NOT available: logs a warning and contributes fakesink (no-op).
class NVUnixFDPublisher : public PipelineElement {
public:
    explicit NVUnixFDPublisher(std::string socket_path);

    void setup(Pipeline* parent) override;

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override;
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    std::string socket_path_;
    std::string gst_string_;
    bool        available_ = false;
};

} // namespace camera_driver
