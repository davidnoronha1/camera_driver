#include "camera_driver/scratchpad.hpp"

namespace camera_driver {

void Scratchpad::set(const std::string& key, std::string value) {
    std::lock_guard<std::mutex> lk(mutex_);
    values_[key] = std::move(value);
}

std::optional<std::string> Scratchpad::get(const std::string& key) const {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = values_.find(key);
    if (it == values_.end()) return std::nullopt;
    return it->second;
}

bool Scratchpad::erase(const std::string& key) {
    std::lock_guard<std::mutex> lk(mutex_);
    return values_.erase(key) > 0;
}

} // namespace camera_driver
