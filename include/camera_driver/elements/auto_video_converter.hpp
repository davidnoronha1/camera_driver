#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>

namespace camera_driver {

// Explicit video conversion element. The user places this deliberately to
// acknowledge that a format conversion is happening (and its latency cost).
// Prevents the "Caps mismatch — add AutoVideoConverterElement()" error.
// If upstream outputs MJPEG, prepends jpegdec. If target is set, appends caps filter.
class AutoVideoConverterElement : public PipelineElement {
public:
    explicit AutoVideoConverterElement(PixelFormat target = PixelFormat::Unknown);

    void setup(Pipeline* parent) override;
    std::string gstString() const override { return gst_string_; }
    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }
    Caps outputCapsFor(PixelFormat input) const override;

private:
    PixelFormat target_;
    std::string gst_string_;
    Caps        output_;
};

} // namespace camera_driver
