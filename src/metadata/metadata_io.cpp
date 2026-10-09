#include "camera_driver/metadata/metadata_io.hpp"
#include <fmt/format.h>
#include <gst/app/gstappsrc.h>
#include <gst/gst.h>
#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace camera_driver::metadata_io {

namespace {

bool hasExtension(const std::string& path, const std::string& ext) {
    if (path.size() < ext.size()) return false;
    return std::equal(ext.rbegin(), ext.rend(), path.rbegin(),
        [](char a, char b) { return std::tolower(a) == std::tolower(b); });
}

std::runtime_error gstError(const std::string& context, GstMessage* msg) {
    GError* gerr = nullptr;
    gchar* dbg = nullptr;
    gst_message_parse_error(msg, &gerr, &dbg);
    std::string text = gerr ? gerr->message : "unknown error";
    if (gerr) g_error_free(gerr);
    g_free(dbg);
    return std::runtime_error("metadata_io: " + context + ": " + text);
}

} // namespace

void writeImageWithMetadata(const std::vector<uint8_t>& jpeg_bytes,
                             const std::string& path,
                             const CameraMetadata& metadata)
{
    std::string desc = fmt::format(
        "appsrc name=src is-live=false format=time caps=image/jpeg ! "
        "jifmux name=mux ! filesink location={}",
        path);

    GError* err = nullptr;
    GstElement* pipeline = gst_parse_launch(desc.c_str(), &err);
    if (!pipeline) {
        std::string msg = err ? err->message : "unknown error";
        if (err) g_error_free(err);
        throw std::runtime_error("metadata_io: failed to build write pipeline: " + msg);
    }

    GstElement* src = gst_bin_get_by_name(GST_BIN(pipeline), "src");
    GstElement* mux = gst_bin_get_by_name(GST_BIN(pipeline), "mux");

    // GST_TAG_COMMENT, not GST_TAG_EXTENDED_COMMENT — see MkvRecorderElement.
    std::string tag_value = "camera-metadata=" + metadata.toYaml();
    GstTagList* tags = gst_tag_list_new(GST_TAG_COMMENT, tag_value.c_str(), nullptr);
    gst_tag_setter_merge_tags(GST_TAG_SETTER(mux), tags, GST_TAG_MERGE_REPLACE);
    gst_tag_list_unref(tags);

    GstBuffer* buffer = gst_buffer_new_allocate(nullptr, jpeg_bytes.size(), nullptr);
    gst_buffer_fill(buffer, 0, jpeg_bytes.data(), jpeg_bytes.size());

    GstBus* bus = gst_element_get_bus(pipeline);
    gst_element_set_state(pipeline, GST_STATE_PLAYING);

    gst_app_src_push_buffer(GST_APP_SRC(src), buffer); // takes buffer ownership
    gst_app_src_end_of_stream(GST_APP_SRC(src));

    GstMessage* msg = gst_bus_timed_pop_filtered(bus, GST_CLOCK_TIME_NONE,
        static_cast<GstMessageType>(GST_MESSAGE_EOS | GST_MESSAGE_ERROR));

    std::exception_ptr pending_error;
    if (msg && GST_MESSAGE_TYPE(msg) == GST_MESSAGE_ERROR) {
        pending_error = std::make_exception_ptr(gstError("write pipeline", msg));
    }
    if (msg) gst_message_unref(msg);

    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(bus);
    gst_object_unref(src);
    gst_object_unref(mux);
    gst_object_unref(pipeline);

    if (pending_error) std::rethrow_exception(pending_error);
}

std::optional<CameraMetadata> readMetadataFromFile(const std::string& path) {
    bool is_jpeg = hasExtension(path, ".jpg") || hasExtension(path, ".jpeg");
    std::string desc = is_jpeg
        ? fmt::format("filesrc location={} ! jpegparse ! fakesink", path)
        : fmt::format("filesrc location={} ! matroskademux name=demux demux. ! fakesink", path);

    GError* err = nullptr;
    GstElement* pipeline = gst_parse_launch(desc.c_str(), &err);
    if (!pipeline) {
        std::string msg = err ? err->message : "unknown error";
        if (err) g_error_free(err);
        throw std::runtime_error("metadata_io: failed to build read pipeline: " + msg);
    }

    GstBus* bus = gst_element_get_bus(pipeline);
    gst_element_set_state(pipeline, GST_STATE_PAUSED);

    std::optional<std::string> comment;
    std::exception_ptr pending_error;
    bool done = false;
    while (!done) {
        GstMessage* msg = gst_bus_timed_pop_filtered(bus, 5 * GST_SECOND,
            static_cast<GstMessageType>(GST_MESSAGE_TAG | GST_MESSAGE_ASYNC_DONE |
                                         GST_MESSAGE_ERROR | GST_MESSAGE_EOS));
        if (!msg) break; // timed out — treat as "no more tags coming"

        switch (GST_MESSAGE_TYPE(msg)) {
            case GST_MESSAGE_TAG: {
                GstTagList* tags = nullptr;
                gst_message_parse_tag(msg, &tags);
                gchar* value = nullptr;
                if (!comment && gst_tag_list_get_string(tags, GST_TAG_COMMENT, &value) && value) {
                    comment = value;
                }
                if (value) g_free(value);
                gst_tag_list_unref(tags);
                break;
            }
            case GST_MESSAGE_ASYNC_DONE:
            case GST_MESSAGE_EOS:
                done = true;
                break;
            case GST_MESSAGE_ERROR:
                pending_error = std::make_exception_ptr(gstError("read pipeline", msg));
                done = true;
                break;
            default:
                break;
        }
        gst_message_unref(msg);
    }

    gst_element_set_state(pipeline, GST_STATE_NULL);
    gst_object_unref(bus);
    gst_object_unref(pipeline);

    if (pending_error) std::rethrow_exception(pending_error);

    const std::string prefix = "camera-metadata=";
    if (!comment || comment->rfind(prefix, 0) != 0) return std::nullopt;
    return CameraMetadata::fromYaml(comment->substr(prefix.size()));
}

} // namespace camera_driver::metadata_io
