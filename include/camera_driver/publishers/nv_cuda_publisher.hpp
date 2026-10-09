#pragma once

#include "../pipeline/pipeline_element.hpp"
#include <cuda.h>
#include <functional>
#include <gst/app/gstappsink.h>
#include <memory>
#include <string>
#include <vector>

namespace camera_driver {

// One GPU-resident image plane. Valid only for the duration of the
// CudaCallback invocation — the same device memory is reused next frame.
//
// Two mutually exclusive representations, distinguished by `array`:
//   - Linear (array == nullptr): `ptr` is a flat device pointer with row
//     pitch `pitch`, addressable directly (cuMemcpy2D, kernel args, etc.).
//     This is what dGPU buffers (NVBUF_MEM_CUDA_DEVICE/UNIFIED, GstCuda) use.
//   - CUDA array/texture (array != nullptr): `ptr`/`pitch` are unset. This is
//     what Jetson block-linear NVMM buffers (NVBUF_MEM_SURFACE_ARRAY, mapped
//     via EGL/cuGraphics interop) use — the data isn't flat device memory,
//     so you must bind it with cuTexObjectCreate/cuSurfObjectCreate.
struct CudaPlane {
    CUdeviceptr ptr    = 0;
    size_t      pitch  = 0;       // bytes per row, including any padding
    CUarray     array  = nullptr; // set instead of ptr/pitch for block-linear planes
    int         width  = 0;       // valid bytes per row (<= pitch); pixel width if array
    int         height = 0;       // rows
};

struct CudaFrame {
    std::vector<CudaPlane> planes; // e.g. NV12: [Y, UV]
    int         width  = 0;
    int         height = 0;
    PixelFormat format = PixelFormat::Unknown;
    int         gpu_id = 0;
};

// NvCUDAPublisher — like CustomPublisher, but hands the callback GPU memory
// instead of a host-side buffer/cv::Mat. Accepts buffers backed by either:
//   - NVIDIA NVMM memory (video/x-raw(memory:NVMM), DeepStream/Jetson
//     NvBufSurface — produced by nvvidconv/nvv4l2*/nvunixfdsrc/etc.), or
//   - GstCudaMemory (video/x-raw(memory:CUDAMemory), desktop gst-plugins-bad
//     nvcodec elements such as cudaupload/nvcuvid).
// The callback gets pointers straight into the upstream buffer wherever
// possible (no copy): dGPU NVMM/GstCuda surfaces are already CUDA-addressable
// and are passed through as-is, and Jetson block-linear NVMM surfaces are
// exposed via EGL/cuGraphics interop as CUDA arrays (see CudaPlane). A
// NvBufSurfTransform copy is used only as a fallback when neither applies.
// Requires CAMERA_DRIVER_WITH_CUDA.
class NvCUDAPublisher : public PipelineElement {
public:
    // Invoked synchronously on the GStreamer streaming thread — do not block,
    // and do not retain the device pointers past the call (they are reused
    // for the next frame). Copy out via cuMemcpy if you need to keep them.
    using CudaCallback = std::function<void(const CudaFrame&)>;

    explicit NvCUDAPublisher(int gpu_id = 0);
    ~NvCUDAPublisher() override;

    // Must be called before Pipeline::build().
    void setCallback(CudaCallback cb);

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    bool isSink() const override { return true; }
    std::string gstString() const override { return gst_string_; }

    std::vector<PixelFormat> preferredInputFormats() const override {
        return { PixelFormat::NV12_NVMM, PixelFormat::NV12 };
    }
    Caps outputCapsFor(PixelFormat /*input*/) const override {
        Caps c; c.is_any = true; return c;
    }

private:
    int          gpu_id_;
    std::string  gst_string_;
    CudaCallback callback_;

    GstAppSink* appsink_ = nullptr;

    // NvBufSurface-based converter for the NVMM path (defined in the .cpp to
    // keep DeepStream's SDK headers out of this public header). Only used as
    // a fallback when the source surface isn't already CUDA-addressable.
    struct NvmmConverter;
    std::unique_ptr<NvmmConverter> nvmm_;

    static GstFlowReturn onNewSample(GstAppSink* sink, gpointer data);
    void handleNvmmBuffer(GstBuffer* buf);
    void handleCudaMemoryBuffer(GstBuffer* buf, GstCaps* caps);

    // Jetson block-linear (NVBUF_MEM_SURFACE_ARRAY) path: maps surfaceList[0]
    // of `in_surf` (an NvBufSurface*, passed as void* to keep DeepStream's
    // SDK headers out of this public header) via EGL/cuGraphics interop and
    // invokes callback_ with CUDA-array planes, no copy. Returns false
    // (no-op otherwise) if interop isn't available/fails, so the caller can
    // fall back to NvBufSurfTransform. No-op stub on non-aarch64 builds.
    bool handleSurfaceArrayBuffer(void* in_surf);
};

} // namespace camera_driver
