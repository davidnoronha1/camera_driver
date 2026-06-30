#pragma once

#include "caps.hpp"
#include <memory>
#include <string>
#include <vector>

namespace camera_driver {

class Pipeline;

class PipelineElement {
public:
    virtual ~PipelineElement() = default;

    // Called after gst_parse_launch() succeeds — element retrieves its GstElement* by name.
    virtual void setup(Pipeline* /*parent*/) {}

    // Called before pipeline teardown — element releases GstElement references.
    virtual void bringdown(Pipeline* /*parent*/) {}

    // Format negotiation — called during Pipeline::build() to propagate caps.
    // Returns ordered preference list (first = most preferred). Empty = accepts any.
    virtual std::vector<PixelFormat> preferredInputFormats() const { return {}; }

    // Given the input format this element receives, what format does it output?
    // Return Caps with is_any=true if it can output anything (e.g. raw pass-through).
    virtual Caps outputCapsFor(PixelFormat /*input*/) const {
        Caps c; c.is_any = true; return c;
    }

    // Returns the GST pipeline string fragment this element contributes.
    // Empty string means this element contributes nothing (no-op).
    virtual std::string gstString() const { return ""; }

    // Whether this element is an UnresolvedSegment requiring resolution.
    virtual bool isUnresolved() const { return false; }

    // Whether this element fans out (i.e. is a MuxElement with branches).
    virtual bool isMux() const { return false; }

    // Whether this element is a pipeline sink (no further elements after it).
    virtual bool isSink() const { return false; }

    const std::string& name() const { return name_; }

protected:
    std::string name_; // unique element ID used as GST element name
};

} // namespace camera_driver
