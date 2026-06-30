cmake_minimum_required(VERSION 3.16)
project(camera_driver CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ── Options ──────────────────────────────────────────────────────────────────
option(CAMERA_DRIVER_WITH_ROS2  "Enable ROS2 publisher and node"  OFF)
option(CAMERA_DRIVER_WITH_RTSP  "Enable RTSP pipeline support"    ON)

# ── Core dependencies ────────────────────────────────────────────────────────
find_package(PkgConfig REQUIRED)

pkg_check_modules(GST REQUIRED
    gstreamer-1.0>=1.18
    gstreamer-app-1.0>=1.18
    gstreamer-video-1.0>=1.18)

find_package(fmt REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(OpenCV REQUIRED COMPONENTS core imgproc imgcodecs videoio highgui)

# ── Core library (no ROS2 dependency) ────────────────────────────────────────
set(CORE_SOURCES
    src/pipeline/pipeline.cpp
    src/elements/gst_element.cpp
    src/elements/v4l2_src_element.cpp
    src/elements/custom_src_element.cpp
    src/elements/optimized_converter.cpp
    src/elements/mux_element.cpp
    src/elements/auto_video_converter.cpp
    src/publishers/mjpeg_publisher.cpp
    src/publishers/nv_unixfd_publisher.cpp
    src/publishers/custom_publisher.cpp
    src/publishers/display_publisher.cpp
    src/pipelines/yaml_pipeline.cpp
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
        ${OpenCV_LIBS}
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

# ── Standalone executable (pure CMake, no ROS2) ──────────────────────────────
add_executable(camera_driver_exe src/main_standalone.cpp)
# --whole-archive forces every object file in the static library to be linked,
# so REGISTER_PIPELINE_ELEMENT static initializers are not pruned by the linker.
target_link_libraries(camera_driver_exe PRIVATE
    -Wl,--whole-archive camera_driver_core -Wl,--no-whole-archive)

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
