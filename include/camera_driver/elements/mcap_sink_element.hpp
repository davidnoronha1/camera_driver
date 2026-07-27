#pragma once

#include "../pipeline/segment.hpp"
#include <chrono>
#include <gst/app/gstappsink.h>
#include <mcap/writer.hpp>
#include <mutex>
#include <string>
#include <unordered_map>

namespace camera_driver {

// Records the incoming video stream (plus, optionally, any number of
// arbitrary named "aux" topics such as GPS or IMU) to an MCAP file. A sink
// element — plugs into MuxElement branches exactly like MkvRecorderElement.
//
// Unlike MkvRecorderElement, video buffers are pulled into C++ via an
// internal appsink (MCAP is not a GStreamer muxer) and written as MCAP
// messages on a fixed video channel (kVideoTopic). The resolved Caps
// (format/width/height/fps) are stored in that channel's metadata map so
// McapSourceElement can reconstruct the right decode chain on playback.
//
// Aux topics have no fixed schema: call writeTopic() from application code
// (analogous to CustomSrcElement::write()/CustomPublisher::read()) with
// already-serialized bytes (e.g. a JSON string) and a real timestamp. Each
// topic gets its own MCAP channel, created lazily on first use.
//
// If the pipeline-level scratchpad has calibration/pose metadata (see
// Scratchpad::kCameraMetadataKey), it's written once at setup() as a
// file-level MCAP Metadata record named "camera-metadata".
class McapSinkElement : public UnresolvedSegment {
public:
    static constexpr const char* kVideoTopic = "camera/frame";

    explicit McapSinkElement(std::string location);
    ~McapSinkElement() override = default;

    bool isSink() const override { return true; }

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    // Write one sample on an arbitrary aux topic (thread-safe). Lazily
    // registers a schema-less channel for `topic` on first use.
    void writeTopic(const std::string& topic, uint64_t timestamp_ns,
                    const uint8_t* data, size_t size,
                    std::string encoding = "json");

    // Nanoseconds elapsed since setup(). Video buffer PTS (running time since
    // the pipeline reaches PLAYING) uses roughly the same basis for a live
    // source, so callers can stamp aux samples with this to keep them in sync
    // with the recorded video without needing access to the GST clock
    // themselves. Not frame-exact, but consistent for "for now" purposes.
    uint64_t nowNs() const;

private:
    std::string location_;
    Caps        resolved_caps_;

    GstAppSink* appsink_ = nullptr;

    std::mutex        mutex_;
    mcap::McapWriter   writer_;
    bool               opened_ = false;
    mcap::ChannelId    video_channel_id_ = 0;
    uint32_t           video_seq_ = 0;
    std::unordered_map<std::string, mcap::ChannelId> aux_channels_;

    std::chrono::steady_clock::time_point start_time_;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
    void handleVideoBuffer(GstBuffer* buf);
};

} // namespace camera_driver
