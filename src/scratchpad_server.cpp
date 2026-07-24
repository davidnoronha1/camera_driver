#include "camera_driver/scratchpad_server.hpp"
#include "camera_driver/lflogger.hpp"
#include <arpa/inet.h>
#include <cstring>
#include <fmt/format.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace camera_driver {

namespace {

constexpr uint8_t kOpGet = 1;
constexpr uint8_t kOpSet = 2;
constexpr uint8_t kStatusOk = 0;
constexpr uint8_t kStatusNotFound = 1;
constexpr size_t  kMaxBlobSize = 64 * 1024 * 1024; // sanity cap, not a protocol limit

bool readExact(int fd, void* buf, size_t len) {
    auto* p = static_cast<uint8_t*>(buf);
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, p + got, len - got, 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

bool writeExact(int fd, const void* buf, size_t len) {
    const auto* p = static_cast<const uint8_t*>(buf);
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool readString16(int fd, std::string& out) {
    uint16_t len_be = 0;
    if (!readExact(fd, &len_be, sizeof(len_be))) return false;
    uint16_t len = ntohs(len_be);
    out.resize(len);
    return len == 0 || readExact(fd, out.data(), len);
}

bool readString32(int fd, std::string& out) {
    uint32_t len_be = 0;
    if (!readExact(fd, &len_be, sizeof(len_be))) return false;
    uint32_t len = ntohl(len_be);
    if (len > kMaxBlobSize) return false;
    out.resize(len);
    return len == 0 || readExact(fd, out.data(), len);
}

bool writeString32(int fd, const std::string& s) {
    uint32_t len_be = htonl(static_cast<uint32_t>(s.size()));
    if (!writeExact(fd, &len_be, sizeof(len_be))) return false;
    return s.empty() || writeExact(fd, s.data(), s.size());
}

sockaddr_un makeAddr(const std::string& path) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    return addr;
}

} // namespace

// ─── ScratchpadServer ───────────────────────────────────────────────────────

ScratchpadServer::ScratchpadServer(std::shared_ptr<Scratchpad> scratchpad, std::string socket_path)
    : scratchpad_(std::move(scratchpad)), socket_path_(std::move(socket_path)) {}

ScratchpadServer::~ScratchpadServer() { stop(); }

void ScratchpadServer::start() {
    listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::runtime_error("ScratchpadServer: socket() failed");

    ::unlink(socket_path_.c_str()); // remove stale socket file from a previous run

    sockaddr_un addr = makeAddr(socket_path_);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
        ::listen(listen_fd_, 8) < 0)
    {
        throw std::runtime_error("ScratchpadServer: cannot bind " + socket_path_);
    }

    running_ = true;
    listen_thread_ = std::thread(&ScratchpadServer::listenLoop, this);

    LockFreeLogger::getInstance().info("scratchpad",
        fmt::format("ScratchpadServer listening on {}", socket_path_));
}

void ScratchpadServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
    if (listen_thread_.joinable()) listen_thread_.join();
    ::unlink(socket_path_.c_str());
}

void ScratchpadServer::listenLoop() {
    while (running_) {
        fd_set fds; FD_ZERO(&fds); FD_SET(listen_fd_, &fds);
        timeval tv{1, 0};
        if (select(listen_fd_ + 1, &fds, nullptr, nullptr, &tv) <= 0) continue;

        int client_fd = accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) continue;

        std::thread([this, client_fd]() { serveClient(client_fd); }).detach();
    }
}

void ScratchpadServer::serveClient(int fd) {
    uint8_t opcode = 0;
    if (readExact(fd, &opcode, 1)) {
        std::string key;
        if (readString16(fd, key)) {
            if (opcode == kOpGet) {
                auto value = scratchpad_->get(key);
                if (value) {
                    uint8_t status = kStatusOk;
                    writeExact(fd, &status, 1);
                    writeString32(fd, *value);
                } else {
                    uint8_t status = kStatusNotFound;
                    writeExact(fd, &status, 1);
                }
            } else if (opcode == kOpSet) {
                std::string value;
                if (readString32(fd, value)) {
                    scratchpad_->set(key, std::move(value));
                    uint8_t status = kStatusOk;
                    writeExact(fd, &status, 1);
                }
            }
        }
    }
    ::close(fd);
}

// ─── ScratchpadClient ───────────────────────────────────────────────────────

ScratchpadClient::ScratchpadClient(std::string socket_path) : socket_path_(std::move(socket_path)) {}

std::optional<std::string> ScratchpadClient::get(const std::string& key) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return std::nullopt;
    sockaddr_un addr = makeAddr(socket_path_);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return std::nullopt;
    }

    std::optional<std::string> result;
    uint8_t opcode = kOpGet;
    uint16_t key_len_be = htons(static_cast<uint16_t>(key.size()));
    if (writeExact(fd, &opcode, 1) && writeExact(fd, &key_len_be, sizeof(key_len_be)) &&
        writeExact(fd, key.data(), key.size()))
    {
        uint8_t status = kStatusNotFound;
        if (readExact(fd, &status, 1) && status == kStatusOk) {
            std::string value;
            if (readString32(fd, value)) result = std::move(value);
        }
    }
    ::close(fd);
    return result;
}

bool ScratchpadClient::set(const std::string& key, const std::string& value) {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return false;
    sockaddr_un addr = makeAddr(socket_path_);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return false;
    }

    bool ok = false;
    uint8_t opcode = kOpSet;
    uint16_t key_len_be = htons(static_cast<uint16_t>(key.size()));
    if (writeExact(fd, &opcode, 1) && writeExact(fd, &key_len_be, sizeof(key_len_be)) &&
        writeExact(fd, key.data(), key.size()) && writeString32(fd, value))
    {
        uint8_t status = 0xFF;
        ok = readExact(fd, &status, 1) && status == kStatusOk;
    }
    ::close(fd);
    return ok;
}

} // namespace camera_driver
