#pragma once

#include <gst/gst.h>
#include <string>

namespace hw {

struct HWCaps {
    bool has_nvjpegenc     = false; // NVIDIA JPEG encoder
    bool has_nvh264enc     = false; // NVIDIA H.264 encoder (desktop/dGPU)
    bool has_nvv4l2h264enc = false; // NVIDIA V4L2 M2M H.264 encoder
    bool has_nvvidconv     = false; // NVIDIA NVMM colorspace converter (nvvidconv or nvvideoconvert)
    bool has_nvunixfdsink  = false; // NVIDIA Unix FD sink (GPU zero-copy)
    bool has_qsvh264enc    = false; // Intel Quick Sync H.264
    bool has_v4l2h264enc   = false; // Generic V4L2 M2M H.264 encoder
    bool has_jpegenc       = false; // Software JPEG encoder
    bool has_x264enc       = false; // Software H.264 encoder (libx264)

    // Actual element name to use for NVMM colorspace conversion.
    // Either "nvvidconv" or "nvvideoconvert" depending on which is installed.
    std::string nvvidconv_name;
};

inline bool gstHasElement(const char* name) {
    GstElementFactory* f = gst_element_factory_find(name);
    if (f) { gst_object_unref(f); return true; }
    return false;
}

inline bool checkElementWorks(const char* pipeline_str) {
    GError* err = nullptr;
    GstElement* p = gst_parse_launch(pipeline_str, &err);
    if (!p) { if (err) g_error_free(err); return false; }
    if (err) { g_error_free(err); gst_object_unref(p); return false; }

    GstStateChangeReturn ret = gst_element_set_state(p, GST_STATE_PAUSED);
    bool ok = false;
    if (ret == GST_STATE_CHANGE_ASYNC || ret == GST_STATE_CHANGE_SUCCESS) {
        GstBus* bus = gst_element_get_bus(p);
        GstMessage* msg = gst_bus_timed_pop_filtered(bus, 1 * GST_SECOND,
            static_cast<GstMessageType>(GST_MESSAGE_ERROR | GST_MESSAGE_ASYNC_DONE | GST_MESSAGE_STATE_CHANGED));
        if (msg) {
            ok = (GST_MESSAGE_TYPE(msg) != GST_MESSAGE_ERROR);
            gst_message_unref(msg);
        }
        gst_object_unref(bus);
    } else {
        ok = (ret == GST_STATE_CHANGE_NO_PREROLL);
    }
    gst_element_set_state(p, GST_STATE_NULL);
    gst_object_unref(p);
    return ok;
}

inline const HWCaps& probe() {
    static HWCaps caps = []() {
        HWCaps c;
        c.has_x264enc       = gstHasElement("x264enc");
        c.has_jpegenc       = gstHasElement("jpegenc");
        c.has_qsvh264enc    = gstHasElement("qsvh264enc");
        c.has_v4l2h264enc   = gstHasElement("v4l2h264enc");
        // Check both names for the NVMM colorspace converter
        if (gstHasElement("nvvidconv")) {
            c.has_nvvidconv   = true;
            c.nvvidconv_name  = "nvvidconv";
        } else if (gstHasElement("nvvideoconvert")) {
            c.has_nvvidconv   = true;
            c.nvvidconv_name  = "nvvideoconvert";
        }
        c.has_nvv4l2h264enc = gstHasElement("nvv4l2h264enc");
        c.has_nvunixfdsink  = gstHasElement("nvunixfdsink");
        c.has_nvh264enc     = gstHasElement("nvh264enc");

        if (c.has_nvjpegenc || gstHasElement("nvjpegenc"))
            c.has_nvjpegenc = checkElementWorks(
                "videotestsrc num-buffers=1 ! videoconvert ! "
                "video/x-raw,format=I420 ! nvjpegenc ! fakesink");

        return c;
    }();
    return caps;
}

} // namespace hw
