#pragma once

// Adapts SymbolTable to the decoder's SymbolResolver interface and records
// newly interned ids so the pipeline can emit SYMBOL_MAPPED events.

#include "flashtick/decoder/sbe_routing_decoder.h"
#include "flashtick/symbol_table.h"

namespace flashtick {

class TableResolver final : public SymbolResolver {
public:
    explicit TableResolver(SymbolTable& t) : table_(t) {}

    uint32_t resolve_u64(uint64_t key) noexcept override {
        const uint32_t before = table_.size();
        const uint32_t id = table_.intern_u64(key);
        if (id != 0 && table_.size() != before) new_id_ = id;
        return id;
    }
    uint32_t resolve_str(const char* s, uint32_t len) noexcept override {
        const uint32_t before = table_.size();
        const uint32_t id = table_.intern(std::string_view(s, len));
        if (id != 0 && table_.size() != before) new_id_ = id;
        return id;
    }

    // Hot: returns and clears the id interned by the last resolve, 0 if none.
    uint32_t take_new_id() noexcept {
        const uint32_t id = new_id_;
        new_id_ = 0;
        return id;
    }
    SymbolTable& table() noexcept { return table_; }

private:
    SymbolTable& table_;
    uint32_t new_id_ = 0;
};

} // namespace flashtick
