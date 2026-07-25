#include "camera_driver/elements/mcap_sink_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>
#include <gst/app/gstappsink.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_mcap_sink_id{0};

// Base format name ignoring NVMM-ness, so pixelFormatFromString() round-trips
// it on the McapSourceElement side (formatName() renders NVMM variants as
// "NV12(NVMM)"/"H264(NVMM)", which pixelFormatFromString() doesn't parse).
// is_nvmm is stored separately in the channel metadata.
std::string baseFormatName(PixelFormat f) {
    switch (f) {
        case PixelFormat::H264:
        case PixelFormat::H264_NVMM: return "H264";
        case PixelFormat::NV12:
        case PixelFormat::NV12_NVMM: return "NV12";
        default: return pixelFormatName(f);
    }
}
} // namespace

McapSinkElement::McapSinkElement(std::string location)
    : UnresolvedSegment("mcap_sink_" + std::to_string(g_mcap_sink_id++), {}, {
          PixelFormat::H264,
          PixelFormat::I420,
          PixelFormat::NV12,
          PixelFormat::RGB,
          PixelFormat::BGR,
          PixelFormat::YUYV,
      })
    , location_(std::move(location))
{
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        PixelFormat input = ctx.upstream_caps.format;
        self->resolved_caps_ = ctx.upstream_caps;

        ResolvedSegment seg;
        seg.name = self->name_;
        seg.input_caps = ctx.upstream_caps;
        seg.output_caps = ctx.upstream_caps;

        // H264(_NVMM) arrives already encoded — just parse it (appsink pulls
        // the raw bitstream). Anything else is raw video; pull it in system
        // memory once it's been converted, same as MkvRecorderElement.
        std::string prefix = (input == PixelFormat::H264 || input == PixelFormat::H264_NVMM)
            ? "h264parse ! "
            : "videoconvert ! ";

        seg.gst_string = fmt::format(
            "{}appsink name={}_sink emit-signals=false sync=false drop=false",
            prefix, self->name_);
        return seg;
    };
    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

void McapSinkElement::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_ + "_sink");
    if (!el) throw std::runtime_error("McapSinkElement: appsink '" + name_ + "_sink' not found");
    appsink_ = GST_APP_SINK(el);
    start_time_ = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lk(mutex_);
        mcap::McapWriterOptions opts("");
        auto status = writer_.open(location_, opts);
        if (!status.ok())
            throw std::runtime_error("McapSinkElement: failed to open '" + location_ + "': " + status.message);
        opened_ = true;

        mcap::Schema schema; // no schema — payload is opaque encoded/raw video bytes
        writer_.addSchema(schema);

        mcap::KeyValueMap meta{
            {"format", baseFormatName(resolved_caps_.format)},
            {"is_nvmm", resolved_caps_.is_nvmm ? "1" : "0"},
            {"width", std::to_string(resolved_caps_.width)},
            {"height", std::to_string(resolved_caps_.height)},
            {"fps_num", std::to_string(resolved_caps_.fps_num)},
            {"fps_den", std::to_string(resolved_caps_.fps_den)},
        };
        mcap::Channel channel(kVideoTopic, "raw", schema.id, meta);
        writer_.addChannel(channel);
        video_channel_id_ = channel.id;
    }

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    LockFreeLogger::getInstance().info("mcap_sink",
        fmt::format("{} recording to {}", name_, location_));
}

void McapSinkElement::bringdown(Pipeline* /*parent*/) {
    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (opened_) {
            writer_.close();
            opened_ = false;
        }
    }
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

uint64_t McapSinkElement::nowNs() const {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start_time_).count());
}

void McapSinkElement::writeTopic(const std::string& topic, uint64_t timestamp_ns,
                                  const uint8_t* data, size_t size, std::string encoding) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!opened_) return;

    auto it = aux_channels_.find(topic);
    mcap::ChannelId channel_id;
    if (it != aux_channels_.end()) {
        channel_id = it->second;
    } else {
        mcap::Schema schema; // schema-less; caller's payload is self-describing (e.g. JSON)
        writer_.addSchema(schema);
        mcap::Channel channel(topic, encoding, schema.id);
        writer_.addChannel(channel);
        channel_id = channel.id;
        aux_channels_.emplace(topic, channel_id);
    }

    mcap::Message msg;
    msg.channelId = channel_id;
    msg.sequence = 0;
    msg.logTime = timestamp_ns;
    msg.publishTime = timestamp_ns;
    msg.data = reinterpret_cast<const std::byte*>(data);
    msg.dataSize = size;

    auto status = writer_.write(msg);
    if (!status.ok()) {
        LockFreeLogger::getInstance().error("mcap_sink",
            fmt::format("{} failed to write topic '{}': {}", name_, topic, status.message));
    }
}

void McapSinkElement::handleVideoBuffer(GstBuffer* buf) {
    GstMapInfo map;
    if (!gst_buffer_map(buf, &map, GST_MAP_READ)) return;

    uint64_t pts_ns = GST_BUFFER_PTS_IS_VALID(buf) ? static_cast<uint64_t>(GST_BUFFER_PTS(buf)) : nowNs();

    {
        std::lock_guard<std::mutex> lk(mutex_);
        if (opened_) {
            mcap::Message msg;
            msg.channelId = video_channel_id_;
            msg.sequence = video_seq_++;
            msg.logTime = pts_ns;
            msg.publishTime = pts_ns;
            msg.data = reinterpret_cast<const std::byte*>(map.data);
            msg.dataSize = map.size;

            auto status = writer_.write(msg);
            if (!status.ok()) {
                LockFreeLogger::getInstance().error("mcap_sink",
                    fmt::format("{} failed to write video frame: {}", name_, status.message));
            }
        }
    }

    gst_buffer_unmap(buf, &map);
}

GstFlowReturn McapSinkElement::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<McapSinkElement*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf = gst_sample_get_buffer(sample);
    if (buf) self->handleVideoBuffer(buf);

    gst_sample_unref(sample);
    return GST_FLOW_OK;
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(McapSinkElement, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::McapSinkElement>(
        cfg["location"].as<std::string>());
});
