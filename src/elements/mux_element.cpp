#include "camera_driver/elements/mux_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/pipeline/pipeline.hpp"
#include <atomic>
#include <fmt/format.h>

namespace camera_driver {

namespace {
static std::atomic<int> g_mux_id{0};
} // namespace

MuxElement::MuxElement() {
    name_ = "tee_" + std::to_string(g_mux_id++);
}

void MuxElement::addBranch(std::vector<std::shared_ptr<PipelineElement>> branch) {
    branches_.push_back(std::move(branch));
}

void MuxElement::buildBranches(const PipelineContext& ctx) {
    // Each branch is resolved independently from upstream_caps
    std::string result = fmt::format("tee name={}", name_);

    for (size_t i = 0; i < branches_.size(); ++i) {
        // Resolve the branch chain
        std::vector<ResolvedSegment> branch_segs;
        PipelineContext branch_ctx = ctx;
        branch_ctx.upstream_caps = ctx.upstream_caps;

        for (size_t j = 0; j < branches_[i].size(); ++j) {
            auto& el = branches_[i][j];
            if (j + 1 < branches_[i].size())
                branch_ctx.downstream_prefs = branches_[i][j + 1]->preferredInputFormats();
            else
                branch_ctx.downstream_prefs = {};

            if (el->isUnresolved()) {
                auto* seg = static_cast<UnresolvedSegment*>(el.get());
                ResolvedSegment r = seg->resolve(branch_ctx);
                branch_ctx.upstream_caps = r.output_caps;
                branch_segs.push_back(std::move(r));
            } else {
                Caps out = el->outputCapsFor(branch_ctx.upstream_caps.format);
                ResolvedSegment r;
                r.name = el->name();
                r.gst_string = el->gstString();
                r.input_caps = branch_ctx.upstream_caps;
                r.output_caps = out;
                branch_ctx.upstream_caps = out;
                if (!r.gst_string.empty()) branch_segs.push_back(std::move(r));
            }
        }

        // Assemble branch string
        std::string branch_str;
        for (const auto& s : branch_segs) {
            if (s.gst_string.empty()) continue;
            if (!branch_str.empty()) branch_str += " ! ";
            branch_str += s.gst_string;
        }

        if (!branch_str.empty())
            result += fmt::format("  {}. ! queue leaky=downstream max-size-buffers=2 ! {}",
                name_, branch_str);
    }

    gst_string_ = result;
    LockFreeLogger::getInstance().info("mux", fmt::format("Tee with {} branches: {}",
        branches_.size(), name_));
}

void MuxElement::setup(Pipeline* parent) {
    // Build branches using the pipeline's resolved upstream caps
    // (This is called after Pipeline::build() has done main-chain resolution)
    // For MuxElement, buildBranches() is called by Pipeline::assemblePipeline() override
    // Each branch element gets setup() called too
    for (auto& branch : branches_)
        for (auto& el : branch)
            el->setup(parent);
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(MuxElement, [](const YAML::Node& cfg) {
    auto mux = std::make_shared<camera_driver::MuxElement>();
    if (cfg["branches"] && cfg["branches"].IsSequence()) {
        for (const auto& branch_node : cfg["branches"]) {
            std::vector<std::shared_ptr<camera_driver::PipelineElement>> branch;
            for (const auto& el_node : branch_node) {
                std::string type = el_node["type"].as<std::string>();
                branch.push_back(camera_driver::ElementRegistry::instance().create(type, el_node));
            }
            mux->addBranch(std::move(branch));
        }
    }
    return mux;
});
