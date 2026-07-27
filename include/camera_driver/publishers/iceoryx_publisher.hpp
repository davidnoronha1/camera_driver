#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <atomic>
#include <gst/app/gstappsink.h>
#include <memory>
#include <string>

namespace iox::popo { class UntypedPublisher; }

namespace camera_driver {

// Publishes the incoming raw video stream onto an iceoryx channel as plain
// IceOryxFrame PODs (memcpy into a shared-memory loan — no MCAP, no
// serialization). A sink element — plugs into MuxElement branches exactly
// like CustomPublisher, which this is modeled on.
//
// `topic` is a "Service/Instance/Event" string (e.g. "Orbbec/Camera/Frame"),
// split into the 3-part iceoryx ServiceDescription.
class IceOryxPublisher : public PipelineElement {
public:
    explicit IceOryxPublisher(std::string topic);
    ~IceOryxPublisher() override;

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override {
        return { PixelFormat::RGB, PixelFormat::BGR, PixelFormat::YUYV, PixelFormat::NV12 };
    }
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    std::string topic_;
    std::string gst_string_;

    GstAppSink* appsink_ = nullptr;
    std::unique_ptr<iox::popo::UntypedPublisher> publisher_;
    std::atomic<uint64_t> sequence_{0};

    // Sibling topic "<Service>/<Instance>/Calibration" — publishes the
    // pipeline-level scratchpad's camera metadata (see
    // Scratchpad::kCameraMetadataKey) once at setup(), as raw CameraMetadata
    // YAML bytes. historyCapacity=1 so late-joining subscribers still get it.
    std::unique_ptr<iox::popo::UntypedPublisher> calibration_publisher_;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
};

} // namespace camera_driver
