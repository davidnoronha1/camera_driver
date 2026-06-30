#pragma once

#include "pipeline_element.hpp"
#include "segment.hpp"
#include "../hw_detect.hpp"
#include <gst/gst.h>
#include <memory>
#include <string>
#include <vector>

namespace camera_driver {

class Pipeline {
public:
    Pipeline();
    virtual ~Pipeline();

    // Add an element to the linear pipeline chain.
    // MuxElement must be the last element added (it terminates the main chain).
    void add(std::shared_ptr<PipelineElement> element);

    // Build: resolve UnresolvedSegments, validate caps, assemble and parse the GST string.
    // Calls setup() on each element after parse succeeds.
    // Throws std::runtime_error on caps mismatch or parse failure.
    virtual void build();

    // Start the pipeline (set state to PLAYING) and block on the GLib main loop.
    virtual void run();

    // Signal the pipeline to stop (thread-safe, can be called from any thread).
    void stop();

    // Returns the fully assembled GST pipeline string (available after build()).
    const std::string& gstString() const { return gst_string_; }

    // Get a named GstElement from the running pipeline (available after build()).
    // Caller must gst_object_unref() the returned element.
    GstElement* getGstElement(const std::string& name) const;

    const hw::HWCaps& hwCaps() const { return hw_; }

protected:
    std::vector<std::shared_ptr<PipelineElement>> elements_;
    std::string  gst_string_;
    hw::HWCaps   hw_;
    GstElement*  gst_pipeline_  = nullptr;
    GMainLoop*   glib_loop_     = nullptr;

    // Resolution internals
    std::vector<ResolvedSegment> resolved_linear_; // resolved segments for the main chain

    virtual void resolveSegments();
    virtual void validateCaps();
    virtual std::string assemblePipeline();

    static gboolean onBusMessage(GstBus* bus, GstMessage* msg, gpointer data);

private:
    // Resolve a linear sequence of PipelineElements into ResolvedSegments.
    // Used both for the main chain and for MuxElement branches.
    std::vector<ResolvedSegment> resolveChain(
        const std::vector<std::shared_ptr<PipelineElement>>& elements,
        const PipelineContext& base_ctx);

    // Assemble a vector of resolved segments into a single GST sub-string.
    static std::string assembleChain(const std::vector<ResolvedSegment>& segs);
};

} // namespace camera_driver
