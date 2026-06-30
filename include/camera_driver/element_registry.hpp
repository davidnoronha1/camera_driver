#pragma once

#include "pipeline/pipeline_element.hpp"
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <yaml-cpp/yaml.h>

namespace camera_driver {

using ElementFactory = std::function<std::shared_ptr<PipelineElement>(const YAML::Node&)>;

class ElementRegistry {
public:
    static ElementRegistry& instance() {
        static ElementRegistry reg;
        return reg;
    }

    void registerElement(const std::string& type_name, ElementFactory factory) {
        factories_[type_name] = std::move(factory);
    }

    std::shared_ptr<PipelineElement> create(const std::string& type, const YAML::Node& cfg) const {
        auto it = factories_.find(type);
        if (it == factories_.end()) {
            std::string known;
            for (auto& [k, _] : factories_) known += " " + k;
            throw std::runtime_error("Unknown element type '" + type + "'. Known types:" + known);
        }
        return it->second(cfg);
    }

private:
    std::unordered_map<std::string, ElementFactory> factories_;
};

} // namespace camera_driver

// Registers a PipelineElement subclass with the ElementRegistry at static init time.
// Place this macro in the element's .cpp file.
// Uses variadic args so lambda bodies with commas inside {} are captured correctly.
#define REGISTER_PIPELINE_ELEMENT(TypeName, ...)                              \
    static bool _cd_reg_##TypeName = []() {                                   \
        ::camera_driver::ElementRegistry::instance().registerElement(          \
            #TypeName, __VA_ARGS__);                                           \
        return true;                                                           \
    }()
