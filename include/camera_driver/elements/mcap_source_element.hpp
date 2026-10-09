#pragma once

#include "../pipeline/segment.hpp"
#include <atomic>
#include <functional>
#include <gst/app/gstappsrc.h>
#include <mcap/reader.hpp>
#include <string>
#include <thread>
#include <unordered_map>

namespace camera_driver {

// Reads back an .mcap file written by McapSinkElement as a pipeline source:
// replays the video channel through hardware decode (mirrors
// MkvPlaybackElement), and — since MCAP can hold any number of aux topics at
// arbitrary rates (unlike a fixed one-row-per-frame sidecar) — fires a
// registered callback for each aux message at its recorded time, interleaved
// with the video by a single timestamp-ordered playback thread.
//
// The video channel's format/dimensions are read from its channel metadata
// (written by McapSinkElement) during the resolver, before setup() — same
// ordering requirement as MkvPlaybackElement, and for the same reason: this
// must run during Pipeline::build()'s resolve pass, before later elements'
// resolvers run.
class McapSourceElement : public UnresolvedSegment {
public:
    using TopicCallback = std::function<void(const std::string& topic, uint64_t timestamp_ns,
                                              const uint8_t* data, size_t size)>;

    explicit McapSourceElement(std::string location);
    ~McapSourceElement() override;

    // Register a callback for one aux topic. Must be called before build().
    void setTopicCallback(const std::string& topic, TopicCallback cb);

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

private:
    std::string location_;
    Caps        resolved_caps_;

    mcap::McapReader reader_;
    mcap::ChannelId  video_channel_id_ = 0;

    std::unordered_map<std::string, TopicCallback> topic_callbacks_;

    GstAppSrc*       appsrc_ = nullptr;
    std::thread      playback_thread_;
    std::atomic<bool> running_{false};

    void playbackLoop();
};

} // namespace camera_driver
