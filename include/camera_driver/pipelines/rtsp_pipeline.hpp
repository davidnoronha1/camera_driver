#pragma once

#ifdef CAMERA_DRIVER_WITH_RTSP

#include "../pipeline/pipeline.hpp"
#include <gst/rtsp-server/rtsp-server.h>
#include <string>

namespace camera_driver {

// RTSPPipeline — extends Pipeline to publish an RTSP stream.
// The pipeline must produce H264 output (validated in build()).
// At run() time, the GST pipeline is handed to a GstRTSPMediaFactory
// rather than played directly.
class RTSPPipeline : public Pipeline {
public:
    RTSPPipeline(int port = 8554, std::string mount_point = "/stream");
    ~RTSPPipeline() override;

    void build() override;
    void run()   override;

    int         port()        const { return port_; }
    std::string mountPoint()  const { return mount_point_; }

protected:
    std::string assemblePipeline() override;

private:
    int         port_;
    std::string mount_point_;

    GstRTSPServer*        rtsp_server_  = nullptr;
    GstRTSPMountPoints*   mount_points_ = nullptr;
};

} // namespace camera_driver

#endif // CAMERA_DRIVER_WITH_RTSP
