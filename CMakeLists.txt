cmake_minimum_required(VERSION 3.17) # FindCUDAToolkit requires 3.17+
project(camera_driver CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ── Options ──────────────────────────────────────────────────────────────────
option(CAMERA_DRIVER_WITH_ROS2  "Enable ROS2 publisher and node"  OFF)
option(CAMERA_DRIVER_WITH_RTSP  "Enable RTSP pipeline support"    ON)
option(CAMERA_DRIVER_WITH_CUDA  "Enable NvCUDAPublisher (NvBufSurface + GstCudaMemory)" OFF)

# Root of the DeepStream SDK install, used to locate nvbufsurface headers/libs
# when CAMERA_DRIVER_WITH_CUDA is enabled.
set(CAMERA_DRIVER_DEEPSTREAM_DIR "/opt/nvidia/deepstream/deepstream" CACHE PATH
    "DeepStream SDK root (expects sources/includes/ and lib/ beneath it)")

# ── Core dependencies ────────────────────────────────────────────────────────
find_package(PkgConfig REQUIRED)

pkg_check_modules(GST REQUIRED
    gstreamer-1.0>=1.18
    gstreamer-app-1.0>=1.18
    gstreamer-video-1.0>=1.18)

find_package(fmt REQUIRED)
find_package(yaml-cpp REQUIRED)
# No OpenCV here, deliberately: this binary can run nvjpegenc (DeepStream),
# and OpenCV's libjpeg link collides with libnvds_lljpeg.so's private jpeg
# symbols at the process's global symbol table, aborting the process. Any
# OpenCV-based frame handling must live in a separate process.

# ── Core library (no ROS2 dependency) ────────────────────────────────────────
set(CORE_SOURCES
    src/pipeline/pipeline.cpp
    src/elements/gst_element.cpp
    src/elements/v4l2_src_element.cpp
    src/elements/rtsp_src_element.cpp
    src/elements/nv_unixfd_src_element.cpp
    src/elements/custom_src_element.cpp
    src/elements/optimized_converter.cpp
    src/elements/mux_element.cpp
    src/elements/auto_video_converter.cpp
    src/elements/mkv_recorder_element.cpp
    src/elements/mkv_playback_element.cpp
    src/elements/undistort_element.cpp
    src/gst/undistort_filter.cpp
    src/publishers/mjpeg_publisher.cpp
    src/publishers/nv_unixfd_publisher.cpp
    src/publishers/custom_publisher.cpp
    src/publishers/display_publisher.cpp
    src/pipelines/yaml_pipeline.cpp
    src/metadata/camera_metadata.cpp
    src/metadata/metadata_io.cpp
    src/scratchpad.cpp
    src/scratchpad_server.cpp
)

add_library(camera_driver_core STATIC ${CORE_SOURCES})

target_include_directories(camera_driver_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${GST_INCLUDE_DIRS})

target_link_libraries(camera_driver_core
    PUBLIC
        fmt::fmt
        yaml-cpp
    PRIVATE
        ${GST_LIBRARIES})

target_compile_options(camera_driver_core PRIVATE ${GST_CFLAGS_OTHER})

# ── Optional: RTSP pipeline ──────────────────────────────────────────────────
if(CAMERA_DRIVER_WITH_RTSP)
    pkg_check_modules(GST_RTSP
        gstreamer-rtsp-1.0>=1.18
        gstreamer-rtsp-server-1.0>=1.18)
endif()

if(CAMERA_DRIVER_WITH_RTSP AND GST_RTSP_FOUND)
    target_sources(camera_driver_core PRIVATE src/pipelines/rtsp_pipeline.cpp)
    target_compile_definitions(camera_driver_core PUBLIC CAMERA_DRIVER_WITH_RTSP)
    target_include_directories(camera_driver_core PRIVATE ${GST_RTSP_INCLUDE_DIRS})
    target_link_libraries(camera_driver_core PRIVATE ${GST_RTSP_LIBRARIES})
elseif(CAMERA_DRIVER_WITH_RTSP AND NOT GST_RTSP_FOUND)
    message(WARNING "gstreamer-rtsp-server-1.0 not found — RTSP support disabled. "
        "Install libgstrtspserver-1.0-dev to enable it.")
endif()

# ── Optional: NvCUDAPublisher (NvBufSurface + GstCudaMemory) ─────────────────
if(CAMERA_DRIVER_WITH_CUDA)
    find_package(CUDAToolkit)
    pkg_check_modules(GST_CUDA gstreamer-cuda-1.0>=1.20)

    find_path(CAMERA_DRIVER_NVBUFSURFACE_INCLUDE_DIR nvbufsurface.h
        PATHS "${CAMERA_DRIVER_DEEPSTREAM_DIR}/sources/includes")
    find_library(CAMERA_DRIVER_NVBUFSURFACE_LIB nvbufsurface
        PATHS "${CAMERA_DRIVER_DEEPSTREAM_DIR}/lib")
    find_library(CAMERA_DRIVER_NVBUFSURFTRANSFORM_LIB nvbufsurftransform
        PATHS "${CAMERA_DRIVER_DEEPSTREAM_DIR}/lib")

    set(CAMERA_DRIVER_CUDA_DEPS_FOUND FALSE)
    if(CUDAToolkit_FOUND AND GST_CUDA_FOUND AND CAMERA_DRIVER_NVBUFSURFACE_INCLUDE_DIR
            AND CAMERA_DRIVER_NVBUFSURFACE_LIB AND CAMERA_DRIVER_NVBUFSURFTRANSFORM_LIB)
        set(CAMERA_DRIVER_CUDA_DEPS_FOUND TRUE)
    endif()
endif()

if(CAMERA_DRIVER_WITH_CUDA AND CAMERA_DRIVER_CUDA_DEPS_FOUND)
    target_sources(camera_driver_core PRIVATE src/publishers/nv_cuda_publisher.cpp)
    target_compile_definitions(camera_driver_core PUBLIC CAMERA_DRIVER_WITH_CUDA)
    target_include_directories(camera_driver_core PRIVATE
        ${GST_CUDA_INCLUDE_DIRS}
        ${CAMERA_DRIVER_NVBUFSURFACE_INCLUDE_DIR})
    target_link_libraries(camera_driver_core PRIVATE
        CUDA::cuda_driver
        CUDA::cudart
        ${GST_CUDA_LIBRARIES}
        ${CAMERA_DRIVER_NVBUFSURFACE_LIB}
        ${CAMERA_DRIVER_NVBUFSURFTRANSFORM_LIB})
elseif(CAMERA_DRIVER_WITH_CUDA AND NOT CAMERA_DRIVER_CUDA_DEPS_FOUND)
    message(WARNING "CUDA/gstreamer-cuda-1.0/DeepStream nvbufsurface deps not found — "
        "NvCUDAPublisher disabled. Install the CUDA toolkit, "
        "libgstreamer-plugins-bad1.0-dev, and set CAMERA_DRIVER_DEEPSTREAM_DIR "
        "to your DeepStream SDK root to enable it.")
endif()

# ── Optional: CUDA display demo (separate process, links OpenCV) ────────────
# Deliberately its own executable, not part of camera_driver_exe: OpenCV (and
# its libjpeg) must not share a process with nvjpegenc — see the comment atop
# CustomPublisher. This binary only touches NvCUDAPublisher, never nvjpegenc,
# so linking OpenCV here is safe.
option(CAMERA_DRIVER_BUILD_CUDA_DEMO "Build examples/cuda_display_demo (requires CAMERA_DRIVER_WITH_CUDA + OpenCV)" OFF)
if(CAMERA_DRIVER_BUILD_CUDA_DEMO)
    if(NOT (CAMERA_DRIVER_WITH_CUDA AND CAMERA_DRIVER_CUDA_DEPS_FOUND))
        message(WARNING "CAMERA_DRIVER_BUILD_CUDA_DEMO requires CAMERA_DRIVER_WITH_CUDA "
            "with all its deps found — cuda_display_demo disabled.")
    else()
        find_package(OpenCV QUIET COMPONENTS core imgproc highgui)
        # red_tint.cu (the demo's red-tint kernel) needs an actual CUDA
        # compiler, which find_package(CUDAToolkit) above does not require.
        find_program(CAMERA_DRIVER_NVCC nvcc HINTS "${CUDAToolkit_BIN_DIR}")
        if(OpenCV_FOUND AND CAMERA_DRIVER_NVCC)
            enable_language(CUDA)
            set(CMAKE_CUDA_STANDARD 17)
            # Covers Turing through Ada (desktop dGPUs); override with
            # -DCMAKE_CUDA_ARCHITECTURES=... for other targets (e.g. Jetson).
            if(NOT CMAKE_CUDA_ARCHITECTURES)
                set(CMAKE_CUDA_ARCHITECTURES 75 80 86 89)
            endif()

            add_executable(cuda_display_demo
                examples/cuda_display_demo.cpp
                examples/red_tint.cu)
            target_include_directories(cuda_display_demo PRIVATE ${OpenCV_INCLUDE_DIRS})
            target_link_libraries(cuda_display_demo PRIVATE
                -Wl,--whole-archive camera_driver_core -Wl,--no-whole-archive
                ${OpenCV_LIBS}
                CUDA::cudart)
        elseif(NOT OpenCV_FOUND)
            message(WARNING "OpenCV not found — cuda_display_demo disabled.")
        else()
            message(WARNING "nvcc not found — cuda_display_demo disabled (its "
                "red-tint kernel needs a CUDA compiler, not just the runtime).")
        endif()
    endif()
endif()

# ── Standalone executable (pure CMake, no ROS2) ──────────────────────────────
add_executable(camera_driver_exe src/main_standalone.cpp)
# --whole-archive forces every object file in the static library to be linked,
# so REGISTER_PIPELINE_ELEMENT static initializers are not pruned by the linker.
target_link_libraries(camera_driver_exe PRIVATE
    -Wl,--whole-archive camera_driver_core -Wl,--no-whole-archive)
# Embed an rpath to any absolute-path libs linked in above (notably the
# DeepStream nvbufsurface/nvbufsurftransform libs under
# CAMERA_DRIVER_DEEPSTREAM_DIR/lib, which live outside the system's ld.so
# search path). Without this, cmake --install strips the build-tree rpath
# and the installed binary fails at runtime with "cannot open shared object
# file" even though it linked fine.
set_target_properties(camera_driver_exe PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)

# ── Optional: ROS2 publisher + ROS2 node ─────────────────────────────────────
if(CAMERA_DRIVER_WITH_ROS2)
    find_package(ament_cmake REQUIRED)
    find_package(rclcpp REQUIRED)
    find_package(sensor_msgs REQUIRED)
    find_package(std_msgs REQUIRED)
    find_package(image_transport REQUIRED)
    find_package(camera_info_manager REQUIRED)

    add_library(camera_driver_ros2 STATIC src/publishers/ros2_publisher.cpp)
    target_compile_definitions(camera_driver_ros2 PUBLIC CAMERA_DRIVER_WITH_ROS2)
    target_link_libraries(camera_driver_ros2 PUBLIC camera_driver_core)
    ament_target_dependencies(camera_driver_ros2
        rclcpp sensor_msgs image_transport camera_info_manager)

    add_executable(camera_driver_ros2_node src/ros2_node.cpp)
    target_compile_definitions(camera_driver_ros2_node PRIVATE CAMERA_DRIVER_WITH_ROS2)
    target_link_libraries(camera_driver_ros2_node PRIVATE camera_driver_ros2)
    ament_target_dependencies(camera_driver_ros2_node
        rclcpp sensor_msgs image_transport camera_info_manager)

    install(TARGETS camera_driver_ros2_node camera_driver_exe
        DESTINATION lib/${PROJECT_NAME})
    install(DIRECTORY launch resource
        DESTINATION share/${PROJECT_NAME})
    install(DIRECTORY config
        DESTINATION share/${PROJECT_NAME})

    if(BUILD_TESTING)
        find_package(ament_lint_auto REQUIRED)
        ament_lint_auto_find_test_dependencies()
    endif()

    ament_package()
else()
    install(TARGETS camera_driver_exe DESTINATION bin)
    install(DIRECTORY include/ DESTINATION include)
    install(DIRECTORY config/ DESTINATION share/camera_driver/config)
endif()
