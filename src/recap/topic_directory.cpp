#include "subplane/recap/topic_directory.h"

namespace subplane {

void TopicDirectory::publish(std::vector<ResolvedInstrument> list) {
    std::unordered_map<std::string, ResolvedInstrument> m;
    m.reserve(list.size());
    for (auto& r : list) {
        std::string key = r.symbol;
        m.insert_or_assign(std::move(key), std::move(r));
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        by_symbol_.swap(m);
    }
    version_.fetch_add(1, std::memory_order_acq_rel);
}

bool TopicDirectory::find(std::string_view symbol, ResolvedInstrument& out) const {
    std::lock_guard<std::mutex> lk(mu_);
    const auto it = by_symbol_.find(std::string(symbol));
    if (it == by_symbol_.end()) return false;
    out = it->second;
    return true;
}

size_t TopicDirectory::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return by_symbol_.size();
}

} // namespace subplane
