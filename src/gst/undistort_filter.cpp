// A real custom GStreamer element (GstVideoFilter subclass), not an
// appsink/appsrc application-level bridge — GStreamer handles buffer
// allocation/negotiation/threading natively, and it appears as one element
// ("camera_driver_undistort") in the pipeline string like any other.
//
// Deliberately plain C++ math, no OpenCV: OpenCV's libjpeg collides with
// nvjpegenc/DeepStream's private libjpeg symbols in this process (see
// CustomPublisher's header and CMakeLists.txt's "No OpenCV here" comment).
// This also rules out gst-plugins-bad's own `cameraundistort` element,
// which depends on libopencv-imgcodecs for exactly that reason.
#include "camera_driver/gst/undistort_filter.hpp"
#include <algorithm>
#include <gst/video/gstvideofilter.h>
#include <gst/video/video.h>
#include <mutex>
#include <vector>

namespace {

// Private per-instance state. Held via a pointer (not inline members) so
// the GObject instance struct — which GLib zero-initializes as raw memory,
// not via C++ constructors — never has to embed non-trivial C++ types
// directly.
struct UndistortImpl {
    camera_driver::CalibrationInfo calib;
    std::vector<float> map_x, map_y;
    int map_width = 0, map_height = 0;
};

double dcoef(const std::vector<double>& d, size_t i) {
    return i < d.size() ? d[i] : 0.0;
}

void buildRemapTable(UndistortImpl& impl, int width, int height) {
    impl.map_x.assign(static_cast<size_t>(width) * height, 0.f);
    impl.map_y.assign(static_cast<size_t>(width) * height, 0.f);

    const auto& K = impl.calib.K;
    double fx = K[0], fy = K[4], cx = K[2], cy = K[5];
    double k1 = dcoef(impl.calib.D, 0), k2 = dcoef(impl.calib.D, 1),
           p1 = dcoef(impl.calib.D, 2), p2 = dcoef(impl.calib.D, 3),
           k3 = dcoef(impl.calib.D, 4);

    for (int v = 0; v < height; ++v) {
        for (int u = 0; u < width; ++u) {
            double x = (u - cx) / fx;
            double y = (v - cy) / fy;
            double r2 = x * x + y * y;
            double radial = 1.0 + k1 * r2 + k2 * r2 * r2 + k3 * r2 * r2 * r2;
            double xd = x * radial + 2 * p1 * x * y + p2 * (r2 + 2 * x * x);
            double yd = y * radial + p1 * (r2 + 2 * y * y) + 2 * p2 * x * y;

            size_t idx = static_cast<size_t>(v) * width + u;
            impl.map_x[idx] = static_cast<float>(xd * fx + cx);
            impl.map_y[idx] = static_cast<float>(yd * fy + cy);
        }
    }
    impl.map_width = width;
    impl.map_height = height;
}

// Bilinear-samples packed RGB (3 bytes/pixel); writes black for
// out-of-bounds coordinates (matches OpenCV's default BORDER_CONSTANT
// behavior for cv::undistort/cv::remap).
void sampleBilinearRGB(const uint8_t* src, int stride, int width, int height,
                        float sx, float sy, uint8_t* out) {
    if (sx < 0.f || sy < 0.f || sx > width - 1 || sy > height - 1) {
        out[0] = out[1] = out[2] = 0;
        return;
    }
    int x0 = static_cast<int>(sx), y0 = static_cast<int>(sy);
    int x1 = std::min(x0 + 1, width - 1), y1 = std::min(y0 + 1, height - 1);
    float fx = sx - x0, fy = sy - y0;
    const uint8_t* p00 = src + static_cast<size_t>(y0) * stride + x0 * 3;
    const uint8_t* p10 = src + static_cast<size_t>(y0) * stride + x1 * 3;
    const uint8_t* p01 = src + static_cast<size_t>(y1) * stride + x0 * 3;
    const uint8_t* p11 = src + static_cast<size_t>(y1) * stride + x1 * 3;
    for (int c = 0; c < 3; ++c) {
        float top = p00[c] * (1 - fx) + p10[c] * fx;
        float bot = p01[c] * (1 - fx) + p11[c] * fx;
        out[c] = static_cast<uint8_t>(top * (1 - fy) + bot * fy + 0.5f);
    }
}

} // namespace

// ─── GObject boilerplate ────────────────────────────────────────────────────

struct _GstCameraDriverUndistort {
    GstVideoFilter parent;
    gpointer priv; // UndistortImpl*
};

struct _GstCameraDriverUndistortClass {
    GstVideoFilterClass parent_class;
};

#define GST_TYPE_CAMERA_DRIVER_UNDISTORT (gst_camera_driver_undistort_get_type())
#define GST_CAMERA_DRIVER_UNDISTORT(obj) \
    (G_TYPE_CHECK_INSTANCE_CAST((obj), GST_TYPE_CAMERA_DRIVER_UNDISTORT, GstCameraDriverUndistort))

typedef struct _GstCameraDriverUndistort GstCameraDriverUndistort;
typedef struct _GstCameraDriverUndistortClass GstCameraDriverUndistortClass;

G_DEFINE_TYPE(GstCameraDriverUndistort, gst_camera_driver_undistort, GST_TYPE_VIDEO_FILTER)

static UndistortImpl& impl(GstCameraDriverUndistort* self) {
    return *static_cast<UndistortImpl*>(self->priv);
}

static GstStaticPadTemplate kSinkTemplate = GST_STATIC_PAD_TEMPLATE(
    "sink", GST_PAD_SINK, GST_PAD_ALWAYS, GST_STATIC_CAPS("video/x-raw,format=RGB"));
static GstStaticPadTemplate kSrcTemplate = GST_STATIC_PAD_TEMPLATE(
    "src", GST_PAD_SRC, GST_PAD_ALWAYS, GST_STATIC_CAPS("video/x-raw,format=RGB"));

static void gst_camera_driver_undistort_init(GstCameraDriverUndistort* self) {
    self->priv = new UndistortImpl();
}

static void gst_camera_driver_undistort_finalize(GObject* object) {
    auto* self = GST_CAMERA_DRIVER_UNDISTORT(object);
    delete static_cast<UndistortImpl*>(self->priv);
    self->priv = nullptr;
    G_OBJECT_CLASS(gst_camera_driver_undistort_parent_class)->finalize(object);
}

static GstFlowReturn gst_camera_driver_undistort_transform_frame(
    GstVideoFilter* filter, GstVideoFrame* in_frame, GstVideoFrame* out_frame)
{
    auto* self = GST_CAMERA_DRIVER_UNDISTORT(filter);
    UndistortImpl& state = impl(self);

    int width  = GST_VIDEO_FRAME_WIDTH(in_frame);
    int height = GST_VIDEO_FRAME_HEIGHT(in_frame);
    if (state.map_width != width || state.map_height != height) {
        buildRemapTable(state, width, height);
    }

    const auto* src = static_cast<const uint8_t*>(GST_VIDEO_FRAME_PLANE_DATA(in_frame, 0));
    auto* dst        = static_cast<uint8_t*>(GST_VIDEO_FRAME_PLANE_DATA(out_frame, 0));
    int src_stride   = GST_VIDEO_FRAME_PLANE_STRIDE(in_frame, 0);
    int dst_stride   = GST_VIDEO_FRAME_PLANE_STRIDE(out_frame, 0);

    for (int v = 0; v < height; ++v) {
        uint8_t* out_row = dst + static_cast<size_t>(v) * dst_stride;
        const float* mx = &state.map_x[static_cast<size_t>(v) * width];
        const float* my = &state.map_y[static_cast<size_t>(v) * width];
        for (int u = 0; u < width; ++u) {
            sampleBilinearRGB(src, src_stride, width, height, mx[u], my[u], out_row + u * 3);
        }
    }
    return GST_FLOW_OK;
}

static void gst_camera_driver_undistort_class_init(GstCameraDriverUndistortClass* klass) {
    GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
    GstElementClass* element_class = GST_ELEMENT_CLASS(klass);
    GstVideoFilterClass* vfilter_class = GST_VIDEO_FILTER_CLASS(klass);

    gobject_class->finalize = gst_camera_driver_undistort_finalize;

    gst_element_class_set_static_metadata(element_class,
        "Camera Driver CPU Undistort", "Filter/Effect/Video",
        "Pinhole radial/tangential lens undistortion (CPU remap, no OpenCV)",
        "camera_driver");
    gst_element_class_add_static_pad_template(element_class, &kSinkTemplate);
    gst_element_class_add_static_pad_template(element_class, &kSrcTemplate);

    vfilter_class->transform_frame = gst_camera_driver_undistort_transform_frame;
}

// ─── Public API ──────────────────────────────────────────────────────────────

namespace camera_driver {

void registerUndistortFilter() {
    static std::once_flag once;
    std::call_once(once, [] {
        gst_element_register(nullptr, kUndistortFilterElementName, GST_RANK_NONE,
                              GST_TYPE_CAMERA_DRIVER_UNDISTORT);
    });
}

void setUndistortFilterCalibration(GstElement* filter, const CalibrationInfo& calib) {
    auto* self = GST_CAMERA_DRIVER_UNDISTORT(filter);
    UndistortImpl& state = impl(self);
    state.calib = calib;
    state.map_width = 0; // force a remap-table rebuild on the next frame
}

} // namespace camera_driver
