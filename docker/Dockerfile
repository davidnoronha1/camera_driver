# DeepStream ships GStreamer 1.20+ with all NVIDIA plugins already installed:
# nvvideoconvert, nvh264enc, nvjpegenc, nvv4l2h264enc (Jetson), nvdec, etc.
# We only need to add our project's build deps on top.
FROM nvcr.io/nvidia/deepstream:9.0-samples-multiarch

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# ── Extra deps not in DeepStream devel ───────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build toolchain
    cmake \
    build-essential \
    pkg-config \
    git \
    # GStreamer dev headers (runtime + plugins already in DeepStream)
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    libgstrtspserver-1.0-dev \
    # fmt + yaml-cpp
    libfmt-dev \
    libyaml-cpp-dev \
    # V4L2 userspace tools (v4l2-ctl, v4l2-compliance)
    v4l-utils \
    # X11 client libs for DisplayPublisher (fpsdisplaysink)
    libx11-dev \
    x11-apps \
    gstreamer1.0-libav
# Software H264 encode/decode fallback (x264enc / avdec_h264, via
# gstreamer1.0-libav above) for when no NVIDIA GPU is present or its hw
# encoder/decoder can't handle a given stream — see hw_detect.hpp. Without
# these, OptimizedConverter and MkvPlaybackElement have nothing to fall
# back to but an error.
#
# The base DeepStream image ships several codec libraries already marked
# "installed" in dpkg's database, but their actual files are missing from
# the image filesystem (verified: dpkg believes them present, but e.g.
# /usr/lib/x86_64-linux-gnu/libx264.so.164 doesn't exist on disk) — a
# broken base image, not a caching artifact. A plain `apt-get install`
# treats "already the newest version" as a no-op and never re-extracts the
# files, so every affected package must be explicitly force-reinstalled.
# Found by chasing `ldd` on libgstx264.so/libgstlibav.so until clean —
# there may be more such gaps for codecs this project doesn't use (e.g.
# audio) that haven't been hit yet.
RUN apt-get install -y --reinstall --no-install-recommends \
    gstreamer1.0-plugins-ugly \
    libx264-164 \
    libvpx9 \
    libmp3lame0 \
    libmpg123-0t64 \
    libx265-199 \
    && ldconfig

# DeepStream ships libcudart/libcuda but not the CUDA dev headers
# (cuda.h, cuda_runtime.h) or nvcc needed to build NvCUDAPublisher. cuda-nvcc
# is small (~30MB) and is what CMake's FindCUDAToolkit uses to locate the
# toolkit — without it, CUDAToolkit_ROOT alone isn't enough for it to
# succeed. Install these matching whatever CUDA version is already baked
# into this image, via the NVIDIA apt repo it already has configured.
RUN CUDA_VER=$(basename "$(readlink -f /usr/local/cuda)" | sed -E 's/cuda-([0-9]+)\.([0-9]+)/\1-\2/') && \
    apt-get install -y --no-install-recommends \
        "cuda-cudart-dev-${CUDA_VER}" \
        "cuda-driver-dev-${CUDA_VER}" \
        "cuda-nvcc-${CUDA_VER}"

# ── Build camera_driver ───────────────────────────────────────────────────────
WORKDIR /workspace/camera_driver
COPY . .

RUN cmake -B build \
        -DCAMERA_DRIVER_WITH_ROS2=OFF \
        -DCAMERA_DRIVER_WITH_RTSP=ON \
        -DCAMERA_DRIVER_WITH_CUDA=ON \
        -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel "$(nproc)" \
    && cmake --install build --prefix /usr/local

# ── Runtime default ───────────────────────────────────────────────────────────
# Override CMD to pass a different pipeline YAML:
#   docker compose run camera_driver /workspace/camera_driver/config/my_pipeline.yaml
CMD ["/usr/local/bin/camera_driver_exe", \
     "/workspace/camera_driver/config/example_pipeline.yaml"]
