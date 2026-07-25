#include "camera_driver/elements/mcap_source_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/elements/mcap_sink_element.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_mcap_source_id{0};
} // namespace

McapSourceElement::McapSourceElement(std::string location)
    : UnresolvedSegment("mcap_source_" + std::to_string(g_mcap_source_id++), {})
    , location_(std::move(location))
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        auto& log = LockFreeLogger::getInstance();

        auto open_status = self->reader_.open(self->location_);
        if (!open_status.ok())
            throw std::runtime_error("McapSourceElement: failed to open '" +
                self->location_ + "': " + open_status.message);

        auto sum_status = self->reader_.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
        if (!sum_status.ok())
            throw std::runtime_error("McapSourceElement: failed to read summary of '" +
                self->location_ + "': " + sum_status.message);

        mcap::ChannelPtr video_channel;
        for (const auto& [id, ch] : self->reader_.channels()) {
            if (ch && ch->topic == McapSinkElement::kVideoTopic) { video_channel = ch; break; }
        }
        if (!video_channel)
            throw std::runtime_error(std::string("McapSourceElement: no '") +
                McapSinkElement::kVideoTopic + "' channel found in " + self->location_);
        self->video_channel_id_ = video_channel->id;

        auto meta = [&](const char* key, const std::string& def) -> std::string {
            auto it = video_channel->metadata.find(key);
            return it != video_channel->metadata.end() ? it->second : def;
        };
        self->resolved_caps_.format   = pixelFormatFromString(meta("format", "Unknown"));
        self->resolved_caps_.is_nvmm  = meta("is_nvmm", "0") == "1";
        self->resolved_caps_.width    = std::stoi(meta("width", "0"));
        self->resolved_caps_.height   = std::stoi(meta("height", "0"));
        self->resolved_caps_.fps_num  = std::stoi(meta("fps_num", "30"));
        self->resolved_caps_.fps_den  = std::stoi(meta("fps_den", "1"));

        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps.is_any = true;

        if (self->resolved_caps_.format == PixelFormat::H264) {
            // nvv4l2decoder is DeepStream's cross-platform (Jetson + dGPU)
            // hardware decoder and outputs NVMM directly; prefer it, then
            // vanilla NVDEC, then software — mirrors MkvPlaybackElement.
            std::string decoder;
            if (ctx.hw.has_nvv4l2h264dec) {
                decoder = "nvv4l2decoder";
                seg.output_caps.format = PixelFormat::NV12_NVMM;
                seg.output_caps.is_nvmm = true;
            } else if (ctx.hw.has_nvh264dec) {
                decoder = "nvh264dec";
                seg.output_caps.format = PixelFormat::NV12;
            } else if (ctx.hw.has_avdec_h264) {
                decoder = "avdec_h264";
                seg.output_caps.format = PixelFormat::I420;
            } else {
                throw std::runtime_error(
                    "McapSourceElement: no H264 decoder available (install NVIDIA "
                    "DeepStream decode plugins, or gstreamer1.0-libav for avdec_h264)");
            }

            seg.gst_string = fmt::format(
                "appsrc name={0}_src format=time is-live=true block=true "
                "caps=video/x-h264,stream-format=byte-stream,alignment=au ! "
                "h264parse ! {1}", self->name_, decoder);
            log.info("mcap_source", fmt::format("{} playing back {} via {}",
                self->name_, self->location_, decoder));
        } else {
            seg.output_caps = self->resolved_caps_;
            seg.gst_string = fmt::format(
                "appsrc name={}_src format=time is-live=true block=true caps={}",
                self->name_, self->resolved_caps_.toGstCapsString());
            log.info("mcap_source", fmt::format("{} playing back {} (raw {})",
                self->name_, self->location_, self->resolved_caps_.formatName()));
        }
        return seg;
    };

    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

McapSourceElement::~McapSourceElement() = default;

void McapSourceElement::setTopicCallback(const std::string& topic, TopicCallback cb) {
    topic_callbacks_[topic] = std::move(cb);
}

void McapSourceElement::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_ + "_src");
    if (!el) throw std::runtime_error("McapSourceElement: appsrc '" + name_ + "_src' not found");
    appsrc_ = GST_APP_SRC(el);
    gst_app_src_set_stream_type(appsrc_, GST_APP_STREAM_TYPE_STREAM);

    running_ = true;
    playback_thread_ = std::thread(&McapSourceElement::playbackLoop, this);

    LockFreeLogger::getInstance().info("mcap_source", fmt::format("{} ready", name_));
}

void McapSourceElement::bringdown(Pipeline* /*parent*/) {
    running_ = false;
    if (playback_thread_.joinable()) playback_thread_.join();
    if (appsrc_) {
        gst_app_src_end_of_stream(appsrc_);
        gst_object_unref(appsrc_);
        appsrc_ = nullptr;
    }
    reader_.close();
}

void McapSourceElement::playbackLoop() {
    struct Msg {
        uint64_t ts;
        mcap::ChannelId channel;
        std::vector<uint8_t> data;
    };
    std::vector<Msg> messages;

    for (const auto& view : reader_.readMessages()) {
        const auto* bytes = reinterpret_cast<const uint8_t*>(view.message.data);
        messages.push_back(Msg{
            static_cast<uint64_t>(view.message.logTime),
            view.message.channelId,
            std::vector<uint8_t>(bytes, bytes + view.message.dataSize)
        });
    }

    auto& log = LockFreeLogger::getInstance();

    if (messages.empty()) {
        log.warn("mcap_source", fmt::format("{} no messages found in {}", name_, location_));
        return;
    }

    std::stable_sort(messages.begin(), messages.end(),
        [](const Msg& a, const Msg& b) { return a.ts < b.ts; });

    const uint64_t first_ts = messages.front().ts;
    const auto start = std::chrono::steady_clock::now();

    for (const auto& m : messages) {
        if (!running_) break;

        // Paced replay: interruptible sleep in small chunks so bringdown()'s
        // running_=false is picked up promptly rather than after a long sleep.
        const auto target = start + std::chrono::nanoseconds(m.ts - first_ts);
        while (running_) {
            auto now = std::chrono::steady_clock::now();
            if (now >= target) break;
            std::this_thread::sleep_for(std::min<std::chrono::nanoseconds>(
                target - now, std::chrono::milliseconds(20)));
        }
        if (!running_) break;

        if (m.channel == video_channel_id_) {
            GstBuffer* buf = gst_buffer_new_allocate(nullptr, m.data.size(), nullptr);
            GstMapInfo map;
            if (gst_buffer_map(buf, &map, GST_MAP_WRITE)) {
                std::memcpy(map.data, m.data.data(), m.data.size());
                gst_buffer_unmap(buf, &map);
            }
            GST_BUFFER_PTS(buf) = m.ts - first_ts;
            gst_app_src_push_buffer(appsrc_, buf);
        } else {
            mcap::ChannelPtr ch = reader_.channel(m.channel);
            if (ch) {
                auto it = topic_callbacks_.find(ch->topic);
                if (it != topic_callbacks_.end())
                    it->second(ch->topic, m.ts, m.data.data(), m.data.size());
            }
        }
    }

    if (appsrc_) gst_app_src_end_of_stream(appsrc_);
    log.info("mcap_source", fmt::format("{} playback finished ({} messages)", name_, messages.size()));
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(McapSourceElement, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::McapSourceElement>(
        cfg["location"].as<std::string>());
});
