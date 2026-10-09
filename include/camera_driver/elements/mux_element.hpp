#pragma once

#include "../pipeline/pipeline_element.hpp"
#include "../pipeline/pipeline_context.hpp"
#include "../pipeline/segment.hpp"
#include <memory>
#include <string>
#include <vector>

namespace camera_driver {

// Wraps GStreamer tee element. Fans a single video stream out to multiple branches.
// Each branch is a linear sequence of PipelineElements ending in a sink.
// MuxElement must be the last element added to Pipeline::add() — it is terminal.
class MuxElement : public PipelineElement {
public:
    MuxElement();

    // Add a branch. Each branch is a chain of elements ending in a sink.
    void addBranch(std::vector<std::shared_ptr<PipelineElement>> branch);

    // Called during Pipeline::build() to resolve branch chains and build gst_string_.
    void buildBranches(const PipelineContext& ctx);

    void setup(Pipeline* parent) override;
    bool isMux() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    std::vector<std::vector<std::shared_ptr<PipelineElement>>> branches_;
    std::string gst_string_;
};

} // namespace camera_driver
