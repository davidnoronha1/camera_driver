#include "camera_driver/publishers/nv_cuda_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"

#include <gst/cuda/gstcudacontext.h>
#include <gst/cuda/gstcudamemory.h>
#include <gst/video/video.h>

#include <cuda_runtime.h>
#include <nvbufsurface.h>
#include <nvbufsurftransform.h>

#if defined(__aarch64__)
#include <cudaEGL.h>
#endif

#include <atomic>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {

std::atomic<int> g_cuda_pub_id{0};

PixelFormat gstVideoFormatToPixelFormat(GstVideoFormat f) {
    switch (f) {
        case GST_VIDEO_FORMAT_NV12: return PixelFormat::NV12;
        case GST_VIDEO_FORMAT_I420: return PixelFormat::I420;
        case GST_VIDEO_FORMAT_RGBA: return PixelFormat::RGBA;
        case GST_VIDEO_FORMAT_RGB:  return PixelFormat::RGB;
        case GST_VIDEO_FORMAT_BGR:  return PixelFormat::BGR;
        default:                    return PixelFormat::Unknown;
    }
}

PixelFormat nvBufColorFormatToPixelFormat(NvBufSurfaceColorFormat f) {
    switch (f) {
        case NVBUF_COLOR_FORMAT_NV12: return PixelFormat::NV12;
        case NVBUF_COLOR_FORMAT_RGBA: return PixelFormat::RGBA;
        case NVBUF_COLOR_FORMAT_RGB:  return PixelFormat::RGB;
        default:                      return PixelFormat::Unknown;
    }
}

} // namespace

// Converts whatever NvBufSurface arrives (any memType, including
// Jetson's EGL-backed NVBUF_MEM_SURFACE_ARRAY) into a persistent
// NVBUF_MEM_CUDA_UNIFIED surface via NvBufSurfTransform. Unified memory is
// directly addressable by CUDA with no further interop step, on both dGPU
// and Jetson. Reused every frame; reallocated only if the source resolution
// changes.
struct NvCUDAPublisher::NvmmConverter {
    NvBufSurface* dest = nullptr;
    int width  = 0;
    int height = 0;

    ~NvmmConverter() {
        if (dest) NvBufSurfaceDestroy(dest);
    }
};

NvCUDAPublisher::NvCUDAPublisher(int gpu_id) : gpu_id_(gpu_id) {
    name_ = "nv_cuda_pub_" + std::to_string(g_cuda_pub_id++);
    gst_string_ = fmt::format(
        "appsink name={} max-buffers=2 drop=true sync=false emit-signals=false",
        name_);
}

NvCUDAPublisher::~NvCUDAPublisher() = default;

void NvCUDAPublisher::setCallback(CudaCallback cb) { callback_ = std::move(cb); }

void NvCUDAPublisher::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("NvCUDAPublisher: appsink '" + name_ + "' not found");
    appsink_ = GST_APP_SINK(el);

    cuInit(0); // idempotent; guarantees the driver API is ready even if no
               // upstream GstCuda element has touched CUDA yet.

    cudaError_t cerr = cudaSetDevice(gpu_id_);
    if (cerr != cudaSuccess) {
        throw std::runtime_error(fmt::format(
            "NvCUDAPublisher: cudaSetDevice({}) failed: {}", gpu_id_, cudaGetErrorString(cerr)));
    }

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    LockFreeLogger::getInstance().info("nv_cuda_pub",
        fmt::format("{} ready (gpu {})", name_, gpu_id_));
}

void NvCUDAPublisher::bringdown(Pipeline* /*parent*/) {
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
    nvmm_.reset();
}

GstFlowReturn NvCUDAPublisher::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<NvCUDAPublisher*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf  = gst_sample_get_buffer(sample);
    GstCaps*   caps = gst_sample_get_caps(sample);

    if (buf && gst_buffer_n_memory(buf) > 0) {
        GstMemory* mem = gst_buffer_peek_memory(buf, 0);
        if (gst_is_cuda_memory(mem)) {
            self->handleCudaMemoryBuffer(buf, caps);
        } else {
            self->handleNvmmBuffer(buf);
        }
    }

    gst_sample_unref(sample);
    return GST_FLOW_OK;
}

// GstCudaMemory path (desktop dGPU: cudaupload, nvcodec decoders, etc.).
// No copy: the callback gets pointers straight into GstCuda's own
// alloc2d-padded planes (native pitch, may include padding beyond the valid
// row bytes), inside the memory's own CUDA context (pushed/popped around the
// callback so it can safely issue further CUDA calls against these
// pointers). Valid only until gst_video_frame_unmap below, i.e. only for the
// duration of the callback.
void NvCUDAPublisher::handleCudaMemoryBuffer(GstBuffer* buf, GstCaps* caps) {
    if (!callback_ || !caps) return;

    GstVideoInfo info;
    if (!gst_video_info_from_caps(&info, caps)) return;

    GstVideoFrame frame;
    if (!gst_video_frame_map(&frame, &info, buf,
            static_cast<GstMapFlags>(GST_MAP_READ | GST_MAP_CUDA))) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " gst_video_frame_map(CUDA) failed");
        return;
    }

    auto* mem = reinterpret_cast<GstCudaMemory*>(gst_buffer_peek_memory(buf, 0));
    const guint n_planes = GST_VIDEO_FRAME_N_PLANES(&frame);

    CudaFrame out;
    out.width  = GST_VIDEO_INFO_WIDTH(&info);
    out.height = GST_VIDEO_INFO_HEIGHT(&info);
    out.gpu_id = gpu_id_;
    out.format = gstVideoFormatToPixelFormat(GST_VIDEO_INFO_FORMAT(&info));
    out.planes.resize(n_planes);
    for (guint i = 0; i < n_planes; i++) {
        size_t row_bytes = static_cast<size_t>(GST_VIDEO_FRAME_COMP_WIDTH(&frame, i)) *
                            GST_VIDEO_FRAME_COMP_PSTRIDE(&frame, i);
        out.planes[i] = {
            reinterpret_cast<CUdeviceptr>(GST_VIDEO_FRAME_PLANE_DATA(&frame, i)),
            static_cast<size_t>(GST_VIDEO_FRAME_PLANE_STRIDE(&frame, i)),
            nullptr,
            static_cast<int>(row_bytes),
            static_cast<int>(GST_VIDEO_FRAME_COMP_HEIGHT(&frame, i)),
        };
    }

    gst_cuda_context_push(mem->context);
    callback_(out);
    gst_cuda_context_pop(nullptr);

    gst_video_frame_unmap(&frame);
}

// NVMM path (DeepStream/Jetson NvBufSurface). If the source surface is
// already CUDA-addressable (dGPU default: NVBUF_MEM_CUDA_DEVICE, or
// NVBUF_MEM_CUDA_UNIFIED) and pitch-laid-out, we hand back pointers straight
// into it — no copy. Otherwise (e.g. Jetson NVBUF_MEM_SURFACE_ARRAY/EGL HW
// buffers, or block-linear layout) NvBufSurfTransform is still required to
// get CUDA-addressable memory, landing the result in a persistent
// NVBUF_MEM_CUDA_UNIFIED surface reused across frames.
void NvCUDAPublisher::handleNvmmBuffer(GstBuffer* buf) {
    if (!callback_) return;

    GstMapInfo map;
    if (!gst_buffer_map(buf, &map, GST_MAP_READ)) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " gst_buffer_map failed");
        return;
    }
    auto* in_surf = reinterpret_cast<NvBufSurface*>(map.data);

    if (in_surf->numFilled == 0) {
        gst_buffer_unmap(buf, &map);
        return;
    }
    NvBufSurfaceParams& src_frame = in_surf->surfaceList[0];

    const bool directly_addressable =
        (in_surf->memType == NVBUF_MEM_CUDA_UNIFIED || in_surf->memType == NVBUF_MEM_CUDA_DEVICE) &&
        src_frame.layout == NVBUF_LAYOUT_PITCH;

    if (directly_addressable) {
        CudaFrame out;
        out.width  = static_cast<int>(src_frame.width);
        out.height = static_cast<int>(src_frame.height);
        out.gpu_id = gpu_id_;
        out.format = nvBufColorFormatToPixelFormat(src_frame.colorFormat);
        out.planes.resize(src_frame.planeParams.num_planes);
        for (uint32_t i = 0; i < src_frame.planeParams.num_planes; i++) {
            out.planes[i] = {
                reinterpret_cast<CUdeviceptr>(
                    static_cast<uint8_t*>(src_frame.dataPtr) + src_frame.planeParams.offset[i]),
                src_frame.planeParams.pitch[i],
                nullptr,
                static_cast<int>(src_frame.planeParams.width[i] * src_frame.planeParams.bytesPerPix[i]),
                static_cast<int>(src_frame.planeParams.height[i]),
            };
        }
        callback_(out);
        gst_buffer_unmap(buf, &map);
        return;
    }

    // Jetson block-linear buffers: map via EGL/cuGraphics interop instead of
    // paying for a full NvBufSurfTransform copy. Falls through to the
    // transform below only if the interop itself fails.
    if (in_surf->memType == NVBUF_MEM_SURFACE_ARRAY) {
        if (handleSurfaceArrayBuffer(in_surf)) {
            gst_buffer_unmap(buf, &map);
            return;
        }
    }

    if (!nvmm_ || nvmm_->width != static_cast<int>(src_frame.width) ||
        nvmm_->height != static_cast<int>(src_frame.height)) {
        auto conv = std::make_unique<NvmmConverter>();
        conv->width  = static_cast<int>(src_frame.width);
        conv->height = static_cast<int>(src_frame.height);

        NvBufSurfaceCreateParams create_params{};
        create_params.gpuId        = static_cast<uint32_t>(gpu_id_);
        create_params.width        = src_frame.width;
        create_params.height       = src_frame.height;
        create_params.isContiguous = true;
        create_params.colorFormat  = src_frame.colorFormat;
        create_params.layout       = NVBUF_LAYOUT_PITCH;
        create_params.memType      = NVBUF_MEM_CUDA_UNIFIED;

        if (NvBufSurfaceCreate(&conv->dest, 1, &create_params) != 0) {
            LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " NvBufSurfaceCreate failed");
            gst_buffer_unmap(buf, &map);
            return;
        }
        nvmm_ = std::move(conv);
    }

    NvBufSurfTransformConfigParams session{};
    session.compute_mode = NvBufSurfTransformCompute_Default;
    session.gpu_id       = gpu_id_;
    session.cuda_stream  = nullptr;
    NvBufSurfTransformSetSessionParams(&session);

    NvBufSurfTransformRect src_rect{};
    src_rect.top = 0; src_rect.left = 0;
    src_rect.width = src_frame.width; src_rect.height = src_frame.height;
    NvBufSurfTransformRect dst_rect = src_rect;
    NvBufSurfTransformParams xform{};
    xform.src_rect       = &src_rect;
    xform.dst_rect       = &dst_rect;
    xform.transform_flag = 0;

    NvBufSurfTransform_Error err = NvBufSurfTransform(in_surf, nvmm_->dest, &xform);
    gst_buffer_unmap(buf, &map);

    if (err != NvBufSurfTransformError_Success) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub",
            fmt::format("{} NvBufSurfTransform failed ({})", name_, static_cast<int>(err)));
        return;
    }

    NvBufSurfaceParams& dest_frame = nvmm_->dest->surfaceList[0];
    CudaFrame out;
    out.width  = static_cast<int>(dest_frame.width);
    out.height = static_cast<int>(dest_frame.height);
    out.gpu_id = gpu_id_;
    out.format = nvBufColorFormatToPixelFormat(dest_frame.colorFormat);
    out.planes.resize(dest_frame.planeParams.num_planes);
    for (uint32_t i = 0; i < dest_frame.planeParams.num_planes; i++) {
        out.planes[i] = {
            reinterpret_cast<CUdeviceptr>(
                static_cast<uint8_t*>(dest_frame.dataPtr) + dest_frame.planeParams.offset[i]),
            dest_frame.planeParams.pitch[i],
            nullptr,
            static_cast<int>(dest_frame.planeParams.width[i] * dest_frame.planeParams.bytesPerPix[i]),
            static_cast<int>(dest_frame.planeParams.height[i]),
        };
    }

    callback_(out);
}

#if defined(__aarch64__)
// Jetson NVBUF_MEM_SURFACE_ARRAY path. These buffers are EGL-backed (often
// block-linear) and aren't directly CUDA-addressable, so DeepStream's own
// plugins (e.g. gst-nvinfer's allocator) reach them via CUDA-EGL interop
// rather than a copy: map the frame to an EGLImage, register that image as a
// CUDA graphics resource, then pull a CUeglFrame out of it. Depending on the
// surface's layout, the returned CUeglFrame is either pitch-linear
// (frame.pPitch — a real device pointer) or a CUDA array (frame.pArray —
// texture/surface memory, see CudaPlane). Both are handed to the callback
// with no copy of the pixel data. Registration is redone every frame since
// the incoming NvBufSurface changes each call.
bool NvCUDAPublisher::handleSurfaceArrayBuffer(void* in_surf_ptr) {
    auto* in_surf = static_cast<NvBufSurface*>(in_surf_ptr);
    if (NvBufSurfaceMapEglImage(in_surf, 0) != 0) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " NvBufSurfaceMapEglImage failed");
        return false;
    }
    NvBufSurfaceParams& src_frame = in_surf->surfaceList[0];

    CUgraphicsResource cuda_resource = nullptr;
    if (cuGraphicsEGLRegisterImage(&cuda_resource, src_frame.mappedAddr.eglImage,
            CU_GRAPHICS_MAP_RESOURCE_FLAGS_NONE) != CUDA_SUCCESS) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " cuGraphicsEGLRegisterImage failed");
        NvBufSurfaceUnMapEglImage(in_surf, 0);
        return false;
    }

    CUeglFrame egl_frame{};
    if (cuGraphicsResourceGetMappedEglFrame(&egl_frame, cuda_resource, 0, 0) != CUDA_SUCCESS) {
        LockFreeLogger::getInstance().warn("nv_cuda_pub", name_ + " cuGraphicsResourceGetMappedEglFrame failed");
        cuGraphicsUnregisterResource(cuda_resource);
        NvBufSurfaceUnMapEglImage(in_surf, 0);
        return false;
    }

    CudaFrame out;
    out.width  = static_cast<int>(src_frame.width);
    out.height = static_cast<int>(src_frame.height);
    out.gpu_id = gpu_id_;
    out.format = nvBufColorFormatToPixelFormat(src_frame.colorFormat);
    out.planes.resize(egl_frame.planeCount);
    for (unsigned int i = 0; i < egl_frame.planeCount; i++) {
        CudaPlane plane;
        plane.width  = static_cast<int>(src_frame.planeParams.width[i] * src_frame.planeParams.bytesPerPix[i]);
        plane.height = static_cast<int>(src_frame.planeParams.height[i]);
        if (egl_frame.frameType == CU_EGL_FRAME_TYPE_PITCH) {
            plane.ptr = reinterpret_cast<CUdeviceptr>(egl_frame.frame.pPitch[i]);
            // CUeglFrame only reports the pitch of plane 0; NV12 chroma rows
            // are the same byte width as luma (half the pixels, 2 bytes/px),
            // so this pitch is correct for both planes in the formats we see
            // here (NV12/I420 from preferredInputFormats()).
            plane.pitch = egl_frame.pitch;
        } else {
            plane.array = egl_frame.frame.pArray[i];
        }
        out.planes[i] = plane;
    }

    callback_(out);

    cuGraphicsUnregisterResource(cuda_resource);
    NvBufSurfaceUnMapEglImage(in_surf, 0);
    return true;
}
#else
bool NvCUDAPublisher::handleSurfaceArrayBuffer(void* /*in_surf*/) { return false; }
#endif

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(NvCUDAPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::NvCUDAPublisher>(cfg["gpu_id"].as<int>(0));
});
