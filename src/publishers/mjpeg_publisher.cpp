#include "camera_driver/publishers/mjpeg_publisher.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <arpa/inet.h>
#include <fmt/format.h>
#include <gst/app/gstappsink.h>
#include <gst/video/video.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_mjpeg_id{0};
} // namespace

MJPEGPublisher::MJPEGPublisher(int port, int jpeg_quality)
    : port_(port), jpeg_quality_(jpeg_quality)
{
    name_       = "mjpeg_pub_" + std::to_string(g_mjpeg_id++);
    gst_string_ = fmt::format(
        "appsink name={} max-buffers=2 drop=true sync=false emit-signals=false",
        name_);
}

MJPEGPublisher::~MJPEGPublisher() {
    running_ = false;
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    if (listen_thread_.joinable()) listen_thread_.join();
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

std::vector<PixelFormat> MJPEGPublisher::preferredInputFormats() const {
    return { PixelFormat::MJPEG };
}

void MJPEGPublisher::setup(Pipeline* parent) {
    GstElement* el = parent->getGstElement(name_);
    if (!el) throw std::runtime_error("MJPEGPublisher: appsink '" + name_ + "' not found");
    appsink_ = GST_APP_SINK(el);

    GstAppSinkCallbacks cbs{};
    cbs.new_sample = onNewSample;
    gst_app_sink_set_callbacks(appsink_, &cbs, this, nullptr);

    // Start HTTP server
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
        ::listen(listen_fd_, 8) < 0)
    {
        throw std::runtime_error(fmt::format("MJPEGPublisher: cannot bind port {}", port_));
    }

    running_ = true;
    listen_thread_ = std::thread(&MJPEGPublisher::listenLoop, this);

    LockFreeLogger::getInstance().info("mjpeg_pub",
        fmt::format("{} HTTP MJPEG server on http://0.0.0.0:{}/", name_, port_));
}

void MJPEGPublisher::bringdown(Pipeline* /*parent*/) {
    running_ = false;
    frame_cv_.notify_all();
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    if (listen_thread_.joinable()) listen_thread_.join();
    if (appsink_) { gst_object_unref(appsink_); appsink_ = nullptr; }
}

void MJPEGPublisher::pushJpegFrame(const uint8_t* data, size_t size) {
    std::lock_guard<std::mutex> lk(frame_mutex_);
    latest_frame_.assign(data, data + size);
    ++frame_id_;
    frame_cv_.notify_all();
}

GstFlowReturn MJPEGPublisher::onNewSample(GstAppSink* sink, gpointer data) {
    auto* self = static_cast<MJPEGPublisher*>(data);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer* buf = gst_sample_get_buffer(sample);
    GstCaps*   caps = gst_sample_get_caps(sample);
    GstMapInfo map;

    if (gst_buffer_map(buf, &map, GST_MAP_READ)) {
        // Detect format from caps
        bool is_jpeg = false;
        if (caps) {
            GstStructure* s = gst_caps_get_structure(caps, 0);
            is_jpeg = g_str_equal(gst_structure_get_name(s), "image/jpeg");
        }

        if (is_jpeg) {
            self->pushJpegFrame(map.data, map.size);
        } else {
            // MJPEGPublisher only accepts MJPEG input (see preferredInputFormats());
            // an upstream JPEG encoder must run before this element.
            LockFreeLogger::getInstance().error("mjpeg_pub",
                fmt::format("{} received non-JPEG buffer; dropping frame "
                            "(check pipeline negotiated an encoder upstream)", self->name_));
        }
        gst_buffer_unmap(buf, &map);
    }

    gst_sample_unref(sample);
    return GST_FLOW_OK;
}

void MJPEGPublisher::listenLoop() {
    while (running_) {
        fd_set fds; FD_ZERO(&fds); FD_SET(listen_fd_, &fds);
        timeval tv{1, 0};
        if (select(listen_fd_ + 1, &fds, nullptr, nullptr, &tv) <= 0) continue;

        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) continue;

        std::thread([this, client_fd]() { serveClient(client_fd); }).detach();
    }
}

void MJPEGPublisher::serveClient(int fd) {
    // Read HTTP request (discard)
    char buf[1024];
    recv(fd, buf, sizeof(buf), 0);

    // Send HTTP headers
    const char* header =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: multipart/x-mixed-replace; boundary=mjpeg_boundary\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: close\r\n\r\n";
    send(fd, header, strlen(header), MSG_NOSIGNAL);

    uint64_t last_id = 0;
    while (running_) {
        std::vector<uint8_t> frame;
        {
            std::unique_lock<std::mutex> lk(frame_mutex_);
            frame_cv_.wait_for(lk, std::chrono::seconds(2),
                [&]{ return frame_id_ != last_id || !running_; });
            if (!running_) break;
            if (frame_id_ == last_id) continue;
            frame = latest_frame_;
            last_id = frame_id_;
        }

        std::string part_header = fmt::format(
            "--mjpeg_boundary\r\n"
            "Content-Type: image/jpeg\r\n"
            "Content-Length: {}\r\n\r\n", frame.size());

        if (send(fd, part_header.c_str(), part_header.size(), MSG_NOSIGNAL) < 0) break;
        if (send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) < 0) break;
        if (send(fd, "\r\n", 2, MSG_NOSIGNAL) < 0) break;
    }
    ::close(fd);
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(MJPEGPublisher, [](const YAML::Node& cfg) {
    return std::make_shared<camera_driver::MJPEGPublisher>(
        cfg["port"].as<int>(8080),
        cfg["quality"].as<int>(85));
});
