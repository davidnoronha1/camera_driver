#pragma once

#include "pipeline_element.hpp"
#include "pipeline_context.hpp"
#include <functional>
#include <stdexcept>

namespace camera_driver {

// A fully resolved GST pipeline segment with declared caps metadata.
struct ResolvedSegment {
    std::string gst_string;  // The GST element string (e.g. "videoconvert ! x264enc")
    Caps        input_caps;  // What this segment consumes (metadata, not injected into string)
    Caps        output_caps; // What this segment produces
    std::string name;        // Human-readable segment name for logging
};

// A pipeline element whose GST string is determined at build() time via a resolver callback.
// Resolver receives the PipelineContext (upstream caps + downstream prefs + hw info)
// and returns a concrete ResolvedSegment.
class UnresolvedSegment : public PipelineElement {
public:
    using Resolver = std::function<ResolvedSegment(const PipelineContext&)>;

    UnresolvedSegment(std::string name, Resolver resolver,
                      std::vector<PixelFormat> preferred_inputs = {})
        : resolver_(std::move(resolver))
        , preferred_inputs_(std::move(preferred_inputs))
    {
        name_ = std::move(name);
    }

    bool isUnresolved() const override { return true; }

    ResolvedSegment resolve(const PipelineContext& ctx) const {
        if (!resolver_) throw std::runtime_error("UnresolvedSegment '" + name_ + "' has no resolver");
        return resolver_(ctx);
    }

    std::vector<PixelFormat> preferredInputFormats() const override {
        return preferred_inputs_;
    }

    void setPreferredInputFormats(std::vector<PixelFormat> f) {
        preferred_inputs_ = std::move(f);
    }

private:
    Resolver resolver_;
    std::vector<PixelFormat> preferred_inputs_;
};

} // namespace camera_driver
