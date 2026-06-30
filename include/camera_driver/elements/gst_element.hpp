#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <string>
#include <vector>

namespace camera_driver {

// Directly appends a named GStreamer element string to the pipeline.
// Probes pad templates at setup() to infer input/output caps.
class GstElement : public PipelineElement {
public:
    explicit GstElement(std::string element_name, std::string properties = "");

    void setup(Pipeline* parent) override;

    std::vector<PixelFormat> preferredInputFormats() const override;
    Caps outputCapsFor(PixelFormat input) const override;
    std::string gstString() const override;

private:
    std::string element_name_;
    std::string properties_;

    // Populated in setup() by probing GstElementFactory pad templates
    std::vector<PixelFormat> inferred_inputs_;
    Caps inferred_output_;
};

} // namespace camera_driver
