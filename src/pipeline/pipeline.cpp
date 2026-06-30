#include "camera_driver/pipeline/pipeline.hpp"
#include "camera_driver/pipeline/pipeline_context.hpp"
#include "camera_driver/elements/mux_element.hpp"
#include "camera_driver/lflogger.hpp"
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

Pipeline::Pipeline() = default;

Pipeline::~Pipeline() {
    if (gst_pipeline_) {
        gst_element_set_state(gst_pipeline_, GST_STATE_NULL);
        gst_object_unref(gst_pipeline_);
        gst_pipeline_ = nullptr;
    }
    if (glib_loop_) {
        g_main_loop_unref(glib_loop_);
        glib_loop_ = nullptr;
    }
}

void Pipeline::add(std::shared_ptr<PipelineElement> element) {
    elements_.push_back(std::move(element));
}

GstElement* Pipeline::getGstElement(const std::string& name) const {
    if (!gst_pipeline_) return nullptr;
    return gst_bin_get_by_name(GST_BIN(gst_pipeline_), name.c_str());
}

void Pipeline::build() {
    auto& log = LockFreeLogger::getInstance();

    hw_ = hw::probe();
    log.info("pipeline", fmt::format(
        "HW: nvvidconv={} ({}) nvv4l2h264enc={} nvh264enc={} nvjpegenc={} qsv={} v4l2enc={} x264={}",
        hw_.has_nvvidconv, hw_.nvvidconv_name, hw_.has_nvv4l2h264enc,
        hw_.has_nvh264enc, hw_.has_nvjpegenc,
        hw_.has_qsvh264enc, hw_.has_v4l2h264enc, hw_.has_x264enc));

    resolveSegments();
    validateCaps();
    gst_string_ = assemblePipeline();

    log.info("pipeline", "GST pipeline: " + gst_string_);

    GError* err = nullptr;
    gst_pipeline_ = gst_parse_launch(gst_string_.c_str(), &err);
    if (!gst_pipeline_ || err) {
        std::string msg = err ? err->message : "unknown error";
        if (err) g_error_free(err);
        throw std::runtime_error("Failed to parse GST pipeline: " + msg);
    }

    for (auto& element : elements_)
        element->setup(this);
}

void Pipeline::run() {
    if (!gst_pipeline_) throw std::runtime_error("Pipeline::run() called before build()");

    GstBus* bus = gst_element_get_bus(gst_pipeline_);
    gst_bus_add_watch(bus, onBusMessage, this);
    gst_object_unref(bus);

    GstStateChangeReturn ret = gst_element_set_state(gst_pipeline_, GST_STATE_PLAYING);
    if (ret == GST_STATE_CHANGE_FAILURE)
        throw std::runtime_error("Failed to set pipeline to PLAYING");

    LockFreeLogger::getInstance().info("pipeline", "Pipeline running");

    glib_loop_ = g_main_loop_new(nullptr, FALSE);
    g_main_loop_run(glib_loop_);

    gst_element_set_state(gst_pipeline_, GST_STATE_NULL);

    for (auto it = elements_.rbegin(); it != elements_.rend(); ++it)
        (*it)->bringdown(this);

    LockFreeLogger::getInstance().info("pipeline", "Pipeline stopped");
}

void Pipeline::stop() {
    if (glib_loop_ && g_main_loop_is_running(glib_loop_))
        g_main_loop_quit(glib_loop_);
}

// ─── Resolution ──────────────────────────────────────────────────────────────

std::vector<ResolvedSegment> Pipeline::resolveChain(
    const std::vector<std::shared_ptr<PipelineElement>>& elements,
    const PipelineContext& base_ctx)
{
    auto& log = LockFreeLogger::getInstance();
    std::vector<ResolvedSegment> resolved;
    PipelineContext ctx = base_ctx;

    for (size_t i = 0; i < elements.size(); ++i) {
        auto& el = elements[i];

        // Compute downstream preferences: next non-unresolved element's preferences
        if (i + 1 < elements.size()) {
            ctx.downstream_prefs = elements[i + 1]->preferredInputFormats();
        } else {
            ctx.downstream_prefs = {};
        }

        if (el->isUnresolved()) {
            auto* seg = static_cast<UnresolvedSegment*>(el.get());
            ResolvedSegment r = seg->resolve(ctx);
            log.info("pipeline", fmt::format("Resolved '{}': {}", r.name, r.gst_string));
            ctx.upstream_caps = r.output_caps;
            // If this was a V4L2 source, pass its formats to context for downstream
            // (V4L2SrcElement populates ctx.v4l2_formats itself in the resolver)
            resolved.push_back(std::move(r));
        } else if (el->isMux()) {
            // Build branches in the MuxElement using current ctx, then record its string.
            auto* mux = static_cast<MuxElement*>(el.get());
            mux->buildBranches(ctx);
            ResolvedSegment r;
            r.name = el->name();
            r.input_caps = ctx.upstream_caps;
            r.gst_string = mux->gstString();
            r.output_caps.is_any = true;
            resolved.push_back(std::move(r));
        } else {
            // Concrete element — get its gst_string and output caps
            Caps out = el->outputCapsFor(ctx.upstream_caps.format);
            ResolvedSegment r;
            r.name = el->name();
            r.input_caps = ctx.upstream_caps;
            r.gst_string = el->gstString();
            r.output_caps = out;
            log.info("pipeline", fmt::format("Added '{}': {}", r.name, r.gst_string));
            ctx.upstream_caps = out;
            if (!r.gst_string.empty())
                resolved.push_back(std::move(r));
        }
    }

    return resolved;
}

void Pipeline::resolveSegments() {
    PipelineContext base_ctx;
    base_ctx.hw = hw_;
    base_ctx.pipeline = this;
    base_ctx.upstream_caps.is_any = true;

    resolved_linear_ = resolveChain(elements_, base_ctx);
}

void Pipeline::validateCaps() {
    auto& log = LockFreeLogger::getInstance();

    for (size_t i = 1; i < resolved_linear_.size(); ++i) {
        const auto& prev = resolved_linear_[i - 1];
        const auto& curr = resolved_linear_[i];

        if (!prev.output_caps.compatibleWith(curr.input_caps) &&
            !curr.input_caps.is_any && !prev.output_caps.is_any)
        {
            std::string msg = fmt::format(
                "Caps mismatch between '{}' (outputs {}) and '{}' (expects {}). "
                "Add AutoVideoConverterElement() between them.",
                prev.name, pixelFormatName(prev.output_caps.format),
                curr.name, pixelFormatName(curr.input_caps.format));
            log.error("pipeline", msg);
            throw std::runtime_error(msg);
        }
    }
}

std::string Pipeline::assembleChain(const std::vector<ResolvedSegment>& segs) {
    std::string result;
    for (const auto& s : segs) {
        if (s.gst_string.empty()) continue;
        if (!result.empty()) result += " ! ";
        result += s.gst_string;
    }
    return result;
}

std::string Pipeline::assemblePipeline() {
    return assembleChain(resolved_linear_);
}

// ─── GLib Bus Handler ─────────────────────────────────────────────────────────

gboolean Pipeline::onBusMessage(GstBus*, GstMessage* msg, gpointer data) {
    auto* self = static_cast<Pipeline*>(data);
    auto& log = LockFreeLogger::getInstance();

    switch (GST_MESSAGE_TYPE(msg)) {
        case GST_MESSAGE_ERROR: {
            GError* err = nullptr;
            gchar*  dbg = nullptr;
            gst_message_parse_error(msg, &err, &dbg);
            log.error("pipeline", fmt::format("GST error: {} ({})",
                err ? err->message : "?", dbg ? dbg : ""));
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            self->stop();
            break;
        }
        case GST_MESSAGE_EOS:
            log.info("pipeline", "GST EOS");
            self->stop();
            break;
        case GST_MESSAGE_WARNING: {
            GError* err = nullptr;
            gchar*  dbg = nullptr;
            gst_message_parse_warning(msg, &err, &dbg);
            log.warn("pipeline", fmt::format("GST warning: {} ({})",
                err ? err->message : "?", dbg ? dbg : ""));
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            break;
        }
        default:
            break;
    }
    return TRUE;
}

} // namespace camera_driver
