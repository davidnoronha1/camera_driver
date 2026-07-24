// Minimal demo: pull frames from NvCUDAPublisher, run a CUDA kernel on them
// in place (red tint, see red_tint.cu) to prove the frame is live GPU
// memory, then copy them from GPU to CPU memory and display with OpenCV.
//
// Deliberately its own executable, not part of camera_driver_exe: OpenCV
// pulls in libjpeg, which collides with DeepStream's libnvds_lljpeg.so (used
// by nvjpegenc) and aborts the process if both are loaded together — see the
// comment atop CustomPublisher. This binary never touches nvjpegenc, only
// NvCUDAPublisher, so linking OpenCV here is safe.
//
// Source is a synthetic videotestsrc uploaded to the GPU via cudaupload, so
// this runs on any dGPU box with no camera attached. Swap the first two
// elements below for a real source (e.g. cudaupload after decoding a real
// stream, or an NVMM source on Jetson/DeepStream) to see a live feed.
//
// Build:  cmake -B build -DCAMERA_DRIVER_WITH_CUDA=ON -DCAMERA_DRIVER_BUILD_CUDA_DEMO=ON
//         cmake --build build --target cuda_display_demo
// Run:    ./build/cuda_display_demo

#include "camera_driver/elements/gst_element.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include "camera_driver/publishers/nv_cuda_publisher.hpp"
#include "red_tint.hpp"

#include <cuda_runtime.h>
#include <gst/gst.h>
#include <opencv2/opencv.hpp>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using camera_driver::CudaFrame;
using camera_driver::PixelFormat;

namespace {

std::shared_ptr<camera_driver::Pipeline> g_pipeline;

void onSignal(int /*sig*/) {
    if (g_pipeline) g_pipeline->stop();
}

// Copies an NV12 CudaFrame (two device planes: Y, then interleaved UV) into
// a tightly-packed host buffer and converts it to BGR for display. Reused
// scratch buffer avoids a per-frame heap allocation.
bool nv12FrameToBgr(const CudaFrame& frame, std::vector<uint8_t>& host_nv12, cv::Mat& bgr_out) {
    if (frame.format != PixelFormat::NV12 || frame.planes.size() != 2) {
        std::cerr << "cuda_display_demo: expected NV12 with 2 planes, got format="
                   << static_cast<int>(frame.format) << " planes=" << frame.planes.size() << "\n";
        return false;
    }

    const auto& y  = frame.planes[0];
    const auto& uv = frame.planes[1];
    if (y.array || uv.array) {
        // Jetson block-linear planes (see CudaPlane) — this demo only handles
        // the linear-pointer case. Bind these via cuTexObjectCreate instead.
        std::cerr << "cuda_display_demo: got CUDA-array (block-linear) planes, "
                     "not handled by this demo\n";
        return false;
    }

    const size_t y_size  = static_cast<size_t>(y.width)  * y.height;
    const size_t uv_size = static_cast<size_t>(uv.width) * uv.height;
    host_nv12.resize(y_size + uv_size);

    // cudaMemcpy2D strips row padding (pitch -> width) in the same call, so
    // the host buffer ends up tightly packed regardless of device pitch.
    cudaError_t err = cudaMemcpy2D(host_nv12.data(), y.width,
        reinterpret_cast<const void*>(y.ptr), y.pitch,
        y.width, y.height, cudaMemcpyDeviceToHost);
    if (err == cudaSuccess) {
        err = cudaMemcpy2D(host_nv12.data() + y_size, uv.width,
            reinterpret_cast<const void*>(uv.ptr), uv.pitch,
            uv.width, uv.height, cudaMemcpyDeviceToHost);
    }
    if (err != cudaSuccess) {
        std::cerr << "cuda_display_demo: cudaMemcpy2D failed: " << cudaGetErrorString(err) << "\n";
        return false;
    }

    // Standard NV12-in-one-Mat layout OpenCV expects: (h * 3/2) rows of w
    // single-byte "pixels" — the bottom third is really the interleaved UV
    // plane, which COLOR_YUV2BGR_NV12 knows how to unpack.
    cv::Mat nv12(frame.height + frame.height / 2, frame.width, CV_8UC1, host_nv12.data());
    cv::cvtColor(nv12, bgr_out, cv::COLOR_YUV2BGR_NV12);
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    gst_init(&argc, &argv);
    std::signal(SIGINT, onSignal);

    auto pipeline = std::make_shared<camera_driver::Pipeline>();
    g_pipeline = pipeline;

    pipeline->add(std::make_shared<camera_driver::GstElement>(
        "videotestsrc", "is-live=true pattern=ball"));
    pipeline->add(std::make_shared<camera_driver::GstElement>(
        "capsfilter", "caps=video/x-raw,format=NV12"));
    pipeline->add(std::make_shared<camera_driver::GstElement>("cudaupload"));
    // Without this, the appsink (which advertises ANY caps) lets negotiation
    // settle on plain system memory — cudaupload becomes a silent no-op
    // download, gst_is_cuda_memory() correctly says "not CUDA memory", and
    // NvCUDAPublisher misreads the system-memory buffer as an NvBufSurface*.
    // Forcing the CUDAMemory feature here keeps the frame on the GPU end to
    // end.
    pipeline->add(std::make_shared<camera_driver::GstElement>(
        "capsfilter", "caps=video/x-raw(memory:CUDAMemory),format=NV12"));

    auto cuda_pub = std::make_shared<camera_driver::NvCUDAPublisher>(/*gpu_id=*/0);

    // Frames cross from the GStreamer streaming thread (where the callback
    // runs) to main via this mutex. OpenCV's X11/highgui backend isn't
    // thread-safe unless the app calls XInitThreads() itself — calling
    // cv::imshow()/cv::waitKey() straight from the callback thread trips an
    // XCB assertion ("multi-threaded client and XInitThreads has not been
    // called") and aborts after the first frame. So imshow/waitKey only ever
    // run on main, below.
    std::mutex frame_mutex;
    cv::Mat    shared_bgr;
    bool       frame_ready = false;

    std::vector<uint8_t> host_nv12; // only touched on the GStreamer thread
    bool logged_first_frame = false;
    cuda_pub->setCallback([&](const CudaFrame& frame) {
        // Runs a CUDA kernel over the frame's chroma plane in place, before
        // any GPU->host copy — proof this is a live, writable device buffer
        // and not just a pass-through, not merely re-coloring the pixels
        // after the fact on the CPU.
        camera_driver_demo::applyRedTint(frame);

        cv::Mat bgr;
        if (!nv12FrameToBgr(frame, host_nv12, bgr)) return;
        if (!logged_first_frame) {
            std::cout << "cuda_display_demo: first frame " << frame.width << "x" << frame.height
                       << " (gpu " << frame.gpu_id << ")\n";
            logged_first_frame = true;
        }
        std::lock_guard<std::mutex> lock(frame_mutex);
        shared_bgr  = std::move(bgr);
        frame_ready = true;
    });

    pipeline->add(cuda_pub);

    try {
        pipeline->build();
    } catch (const std::exception& e) {
        std::cerr << "cuda_display_demo: fatal: " << e.what() << "\n";
        return 1;
    }

    std::atomic<bool> running{true};
    std::thread gst_thread([&] {
        pipeline->run(); // blocks until stop()/EOS/error
        running = false;
    });

    while (running.load()) {
        cv::Mat frame_to_show;
        {
            std::lock_guard<std::mutex> lock(frame_mutex);
            if (frame_ready) {
                frame_to_show = std::move(shared_bgr);
                frame_ready   = false;
            }
        }
        if (!frame_to_show.empty())
            cv::imshow("NvCUDAPublisher demo (CUDA red tint)", frame_to_show);

        // Also pumps the highgui event loop even when no new frame arrived.
        int key = cv::waitKey(15);
        if (key == 'q' || key == 27) {
            pipeline->stop();
            break;
        }
    }

    gst_thread.join();
    cv::destroyAllWindows();
    return 0;
}
