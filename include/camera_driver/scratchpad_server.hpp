#pragma once

#include "scratchpad.hpp"
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>

namespace camera_driver {

// Exposes a Scratchpad to other processes over a Unix domain socket, so
// consumers that aren't part of this Pipeline's process (e.g. a separate
// tool reading camera-metadata, or a future non-GStreamer consumer) can
// still get at it — same generic get/set semantics as Scratchpad, just
// over a socket. Wire protocol (all integers big-endian):
//   Request:  1B opcode (1=GET, 2=SET) | 2B key_len | key_len bytes key
//             | [SET only] 4B value_len | value_len bytes value
//   Response: GET: 1B status (0=ok, 1=not_found) | [ok] 4B value_len | value bytes
//             SET: 1B status (0=ok)
class ScratchpadServer {
public:
    ScratchpadServer(std::shared_ptr<Scratchpad> scratchpad, std::string socket_path);
    ~ScratchpadServer();

    void start();
    void stop();

private:
    std::shared_ptr<Scratchpad> scratchpad_;
    std::string socket_path_;
    int listen_fd_ = -1;
    std::thread listen_thread_;
    std::atomic<bool> running_{false};

    void listenLoop();
    void serveClient(int client_fd);
};

// Client side of the above protocol. Each call opens a short-lived
// connection — this is a low-frequency scratchpad, not a streaming
// channel, so per-call connect overhead is not a concern.
class ScratchpadClient {
public:
    explicit ScratchpadClient(std::string socket_path);

    std::optional<std::string> get(const std::string& key);
    bool set(const std::string& key, const std::string& value);

private:
    std::string socket_path_;
};

} // namespace camera_driver
