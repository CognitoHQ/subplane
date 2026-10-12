#include "flashtick/core/filter_table.h"

namespace flashtick {

std::unique_ptr<FilterTable> build_filter(const FilterSpec& spec, uint32_t max_symbols,
                                          uint32_t generation, std::string& err) {
    auto t = std::make_unique<FilterTable>();
    t->symbol_bits.assign((static_cast<std::size_t>(max_symbols) + 63) / 64, 0);
    for (uint32_t id : spec.symbol_ids) {
        if (id >= max_symbols) {
            err = "symbol id " + std::to_string(id) + " out of range (max_symbols=" +
                  std::to_string(max_symbols) + ")";
            return nullptr;
        }
        t->symbol_bits[id >> 6] |= 1ull << (id & 63u);
    }
    for (uint8_t m : spec.msg_types) {
        if (m >= 64) {
            err = "msg_type " + std::to_string(m) + " must be < 64";
            return nullptr;
        }
        t->msg_type_mask |= 1ull << m;
    }
    for (uint16_t p : spec.partition_ids) {
        if (p >= 64) {
            err = "partition id " + std::to_string(p) + " must be < 64";
            return nullptr;
        }
        t->partition_mask |= 1ull << p;
    }
    if (!spec.venue_ids.empty()) {
        t->venue_mask = 0;
        for (uint16_t v : spec.venue_ids) {
            if (v >= 64) {
                err = "venue id " + std::to_string(v) + " must be < 64 when venues are filtered";
                return nullptr;
            }
            t->venue_mask |= 1ull << v;
        }
    }
    t->all_symbols   = spec.all_symbols;
    t->bus_admin     = spec.bus_admin;
    t->agent_control = spec.agent_control;
    t->generation    = generation;
    return t;
}

} // namespace flashtick
