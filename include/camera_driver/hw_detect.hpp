#pragma once

#include <fmt/format.h>
#include <fstream>
#include <gst/gst.h>
#include <string>

namespace hw {

struct HWCaps {
    bool has_nvjpegenc     = false; // NVIDIA JPEG encoder
    bool has_nvh264enc     = false; // NVIDIA H.264 encoder (desktop/dGPU)
    bool has_nvv4l2h264enc = false; // NVIDIA V4L2 M2M H.264 encoder
    bool has_nvvidconv     = false; // NVIDIA NVMM colorspace converter (nvvidconv or nvvideoconvert)
    bool has_nvunixfdsink  = false; // NVIDIA Unix FD sink (GPU zero-copy)
    bool has_nvunixfdsrc   = false; // NVIDIA Unix FD source (GPU zero-copy)
    bool has_qsvh264enc    = false; // Intel Quick Sync H.264
    bool has_v4l2h264enc   = false; // Generic V4L2 M2M H.264 encoder
    bool has_jpegenc       = false; // Software JPEG encoder
    bool has_x264enc       = false; // Software H.264 encoder (libx264)

    bool has_nvv4l2h264dec = false; // NVIDIA V4L2 M2M H.264 decoder (Jetson + dGPU, DeepStream)
    bool has_nvh264dec     = false; // NVDEC H.264 decoder (desktop/dGPU, vanilla nvcodec)
    bool has_avdec_h264    = false; // Software H.264 decoder (libav)
    bool has_nvdewarper    = false; // DeepStream lens correction/dewarp element

    // Actual element name to use for NVMM colorspace conversion.
    // Either "nvvidconv" or "nvvideoconvert" depending on which is installed.
    std::string nvvidconv_name;
};

inline bool gstHasElement(const char* name) {
    GstElementFactory* f = gst_element_factory_find(name);
    if (f) { gst_object_unref(f); return true; }
    return false;
}

// Registry presence (gstHasElement) only proves a plugin *loaded* — it does
// not prove the underlying hardware/driver actually supports the operation.
// Verified empirically on real NVIDIA hardware: nvv4l2decoder registers
// fine and gst_element_factory_find("nvv4l2decoder") succeeds, but actually
// running it can still fail at data-flow time with e.g. "Feature not
// supported on this GPU" (GStreamer error code 801) — a real GPU, wrong
// capability. This is exactly the class of false positive that would make
// an element pick a GPU path and then fail at runtime instead of falling
// back to software. checkElementWorks() runs a real (bounded) pipeline and
// only trusts an ERROR/ASYNC_DONE/EOS outcome — it deliberately does NOT
// stop on the first GST_MESSAGE_STATE_CHANGED, since those fire constantly
// during a normal PAUSED transition and can arrive before the real
// error, which would otherwise make this check falsely report success.
inline bool checkElementWorks(const std::string& pipeline_str, int timeout_ms = 1500) {
    GError* err = nullptr;
    GstElement* p = gst_parse_launch(pipeline_str.c_str(), &err);
    if (!p) { if (err) g_error_free(err); return false; }
    if (err) { g_error_free(err); gst_object_unref(p); return false; }

    GstStateChangeReturn ret = gst_element_set_state(p, GST_STATE_PAUSED);
    bool ok = false;
    if (ret == GST_STATE_CHANGE_ASYNC || ret == GST_STATE_CHANGE_SUCCESS) {
        GstBus* bus = gst_element_get_bus(p);
        gint64 deadline = g_get_monotonic_time() + static_cast<gint64>(timeout_ms) * 1000;
        for (;;) {
            gint64 remaining = deadline - g_get_monotonic_time();
            if (remaining <= 0) { ok = false; break; } // timed out — treat as "doesn't work"
            GstMessage* msg = gst_bus_timed_pop_filtered(bus, static_cast<GstClockTime>(remaining) * 1000,
                static_cast<GstMessageType>(GST_MESSAGE_ERROR | GST_MESSAGE_ASYNC_DONE | GST_MESSAGE_EOS));
            if (!msg) { ok = false; break; } // timed out
            GstMessageType type = GST_MESSAGE_TYPE(msg);
            gst_message_unref(msg);
            if (type == GST_MESSAGE_ERROR) { ok = false; break; }
            if (type == GST_MESSAGE_ASYNC_DONE || type == GST_MESSAGE_EOS) { ok = true; break; }
        }
        gst_object_unref(bus);
    } else {
        ok = (ret == GST_STATE_CHANGE_NO_PREROLL);
    }
    gst_element_set_state(p, GST_STATE_NULL);
    gst_object_unref(p);
    return ok;
}

// Writes a minimal valid nvdewarper config (projection-type=3, all-zero
// distortion) purely to probe whether the element functions at all —
// nvdewarper requires a config-file to even start, and an empty/missing
// one fails to parse rather than falling back gracefully.
inline std::string writeProbeDewarperConfig() {
    std::string path = "/tmp/camera_driver_hwprobe_dewarper.cfg";
    std::ofstream out(path);
    out << "[property]\noutput-width=64\noutput-height=64\nnum-batch-buffers=1\n"
           "[surface0]\nprojection-type=3\nsurface-index=0\nwidth=64\nheight=64\n"
           "focal-length=100;100\nsrc-x0=32\nsrc-y0=32\ndistortion=0;0;0;0;0\n";
    return path;
}

inline const HWCaps& probe() {
    static HWCaps caps = []() {
        HWCaps c;

        // ── Pure software elements: no hardware/driver to fail, registry
        // presence is a sufficient and accurate signal. ──────────────────
        c.has_x264enc    = gstHasElement("x264enc");
        c.has_jpegenc    = gstHasElement("jpegenc");
        c.has_avdec_h264 = gstHasElement("avdec_h264");

        // Hardware encoders this codebase doesn't have a test environment
        // for in this session (no Intel/generic-V4L2 hardware available to
        // verify against) — left as registry-only checks, same false-
        // positive risk as everything below in principle, just unverified.
        c.has_qsvh264enc  = gstHasElement("qsvh264enc");
        c.has_v4l2h264enc = gstHasElement("v4l2h264enc");

        // ── NVIDIA: functionally verified, not just registry-checked. ───
        // Each check depends on the previous ones actually working, since
        // e.g. testing the H264 decoder needs a real encoder to produce
        // valid input, and testing the encoder needs a working NVMM
        // colorspace path first — a broken link anywhere in that chain
        // correctly makes everything downstream of it unavailable too,
        // which is exactly the conservative behavior we want (prefer a
        // false "unavailable" over a false "works").
        if (gstHasElement("nvvidconv")) {
            c.nvvidconv_name = "nvvidconv";
        } else if (gstHasElement("nvvideoconvert")) {
            c.nvvidconv_name = "nvvideoconvert";
        }
        if (!c.nvvidconv_name.empty()) {
            c.has_nvvidconv = checkElementWorks(fmt::format(
                "videotestsrc num-buffers=1 ! video/x-raw,width=320,height=240 ! {} ! "
                "video/x-raw(memory:NVMM),format=NV12 ! fakesink", c.nvvidconv_name));
        }

        if (c.has_nvvidconv && gstHasElement("nvv4l2h264enc")) {
            c.has_nvv4l2h264enc = checkElementWorks(fmt::format(
                "videotestsrc num-buffers=1 ! video/x-raw,width=320,height=240 ! {} ! "
                "video/x-raw(memory:NVMM),format=NV12 ! nvv4l2h264enc ! fakesink", c.nvvidconv_name));
        }

        if (gstHasElement("nvh264enc")) {
            c.has_nvh264enc = checkElementWorks(
                "videotestsrc num-buffers=1 ! video/x-raw,width=320,height=240 ! videoconvert ! "
                "video/x-raw,format=NV12 ! nvh264enc ! fakesink");
        }

        c.has_nvunixfdsink = gstHasElement("nvunixfdsink"); // IPC framing, not GPU compute — registry check is enough
        c.has_nvunixfdsrc  = gstHasElement("nvunixfdsrc");

        if (gstHasElement("nvjpegenc")) {
            c.has_nvjpegenc = checkElementWorks(
                "videotestsrc num-buffers=1 ! videoconvert ! "
                "video/x-raw,format=I420 ! nvjpegenc ! fakesink");
        }

        // Decoders: need real encoded H264 to decode, so borrow whichever
        // encoder above was just proven to actually work. Without one, we
        // cannot meaningfully test decode — stay conservatively false
        // rather than falling back to registry-only (which is what caused
        // the false positive this whole check exists to prevent).
        std::string encode_prefix;
        if (c.has_nvv4l2h264enc) {
            encode_prefix = fmt::format("{} ! video/x-raw(memory:NVMM),format=NV12 ! nvv4l2h264enc ! h264parse ! ",
                                         c.nvvidconv_name);
        } else if (c.has_x264enc) {
            encode_prefix = "x264enc ! h264parse ! ";
        }

        if (!encode_prefix.empty() && gstHasElement("nvv4l2decoder")) {
            c.has_nvv4l2h264dec = checkElementWorks(
                "videotestsrc num-buffers=1 ! video/x-raw,width=320,height=240 ! " +
                encode_prefix + "nvv4l2decoder ! fakesink");
        }
        if (!encode_prefix.empty() && gstHasElement("nvh264dec")) {
            c.has_nvh264dec = checkElementWorks(
                "videotestsrc num-buffers=1 ! video/x-raw,width=320,height=240 ! " +
                encode_prefix + "nvh264dec ! fakesink");
        }

        // nvdewarper: needs a config file to even start (see
        // writeProbeDewarperConfig) and NVMM RGBA input, so it can only be
        // meaningfully tested once nvvidconv is already known to work.
        if (c.has_nvvidconv && gstHasElement("nvdewarper")) {
            std::string config_path = writeProbeDewarperConfig();
            c.has_nvdewarper = checkElementWorks(fmt::format(
                "videotestsrc num-buffers=1 ! video/x-raw,width=64,height=64 ! {} ! "
                "video/x-raw(memory:NVMM),format=RGBA ! nvdewarper config-file={} ! fakesink",
                c.nvvidconv_name, config_path));
        }

        return c;
    }();
    return caps;
}

} // namespace hw
