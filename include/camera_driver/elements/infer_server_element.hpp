#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>
#include <vector>

namespace camera_driver {

// Wraps nvinferserver (DeepStream's Triton inference-server bridge). Same
// pad-template probing as GstElement, but with a purpose-named config_file
// property instead of a raw `properties:` string. Does not consume/republish
// inference metadata — that's decided by whatever comes after it in the
// pipeline (raw GstElement escape hatch, or a future dedicated sink).
class InferServerElement : public PipelineElement {
public:
    explicit InferServerElement(std::string config_file);

    void setup(Pipeline* parent) override;

    std::vector<PixelFormat> preferredInputFormats() const override;
    Caps outputCapsFor(PixelFormat input) const override;
    std::string gstString() const override;

private:
    std::string config_file_;

    // Populated in setup() by probing GstElementFactory pad templates
    std::vector<PixelFormat> inferred_inputs_;
};

} // namespace camera_driver
