#pragma once

#include "pipeline_element.hpp"
#include "segment.hpp"
#include "../hw_detect.hpp"
#include "../scratchpad.hpp"
#include "../scratchpad_server.hpp"
#include <atomic>
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

    // Start the pipeline (set state to PLAYING) without blocking. Pairs with
    // tick() for embedding into an external event loop instead of run()'s
    // blocking g_main_loop_run(). Don't mix start()/tick() with run() on the
    // same instance.
    virtual void start();

    // Pump whatever bus messages / GLib main-context work is currently
    // pending, without blocking. Call this periodically from an external
    // event loop after start(), e.g.:
    //   pipeline.start();
    //   while (pipeline.tick()) { /* ...do other event-loop work... */ }
    // Returns false once stop() has been called or the pipeline hit EOS/an
    // error — tick() performs the same teardown run() does after its loop
    // exits the first time it observes this, so no separate call is needed.
    virtual bool tick();

    // Signal the pipeline to stop (thread-safe, can be called from any thread).
    void stop();

    // Returns the fully assembled GST pipeline string (available after build()).
    const std::string& gstString() const { return gst_string_; }

    // Get a named GstElement from the running pipeline (available after build()).
    // Caller must gst_object_unref() the returned element.
    // Explicitly ::-qualified: an unqualified `GstElement` here would be
    // ambiguous in any translation unit that also sees
    // camera_driver::GstElement (elements/gst_element.hpp) declared first,
    // silently resolving to the wrong type depending on #include order.
    ::GstElement* getGstElement(const std::string& name) const;

    const hw::HWCaps& hwCaps() const { return hw_; }

    // Generic key-value store shared by every element added to this
    // pipeline — see Scratchpad. E.g. MkvPlaybackElement publishes
    // calibration/pose here after reading a recording back, and
    // UndistortElement reads it from here automatically; neither needs it
    // manually passed in. Reachable from resolvers too via
    // PipelineContext::pipeline->scratchpad().
    const std::shared_ptr<Scratchpad>& scratchpad() const { return scratchpad_; }

    // Optionally expose the scratchpad to other processes over a Unix
    // socket (see ScratchpadServer). No-op if already started.
    void startScratchpadServer(const std::string& socket_path);

protected:
    std::vector<std::shared_ptr<PipelineElement>> elements_;
    std::string  gst_string_;
    hw::HWCaps   hw_;
    std::shared_ptr<Scratchpad> scratchpad_ = std::make_shared<Scratchpad>();
    std::unique_ptr<ScratchpadServer> scratchpad_server_;
    ::GstElement* gst_pipeline_ = nullptr;
    GMainLoop*   glib_loop_     = nullptr; // only created by run()
    // Private context for the bus watch (created lazily by start()) — not
    // the process's global default context. Sharing the global default
    // context with a GUI toolkit (e.g. OpenCV's GTK/X11 highgui backend)
    // means two threads (the GStreamer bus-watch thread and the GUI's own
    // thread) both try to own/dispatch the same GMainContext, which can
    // wedge the GUI thread indefinitely. tick()/run() iterate this instead.
    GMainContext* glib_context_ = nullptr;

    std::atomic<bool> running_{false};
    bool torn_down_ = false;

    // Resolution internals
    std::vector<ResolvedSegment> resolved_linear_; // resolved segments for the main chain

    virtual void resolveSegments();
    virtual void validateCaps();
    virtual std::string assemblePipeline();

    // Sets state to NULL and calls bringdown() on all elements, in reverse
    // order. Shared by run()'s post-loop cleanup and tick()'s lazy teardown.
    // Idempotent (guarded by torn_down_).
    void teardown();

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
