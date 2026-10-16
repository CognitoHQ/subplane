#pragma once

// A partition is one activation: transport subscription + framing + decoder
// for one bus partition. Built on the cold path from YAML; its hot state
// (sequence tracking, counters, attached sessions) is owned by exactly one
// pipeline thread.

#include "subplane/core/counters.h"
#include "subplane/decoder/dbn_decoder.h"
#include "subplane/decoder/flatbuffers_decoder.h"
#include "subplane/decoder/sbe_routing_decoder.h"
#include "subplane/decoder/sbe_schema.h"
#include "subplane/framing/framing.h"
#include "subplane/recap/recap_config.h"
#include "subplane/recap/recap_transport.h"
#include "subplane/transport/transport.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace subplane {

struct Session;
class RecapState;

struct PartitionConfig {
    std::string name;
    uint16_t    id = 0;
    std::string transport_type;
    YAML::Node  transport_cfg;
    YAML::Node  framing_node;
    YAML::Node  decoder_node;
    YAML::Node  recap_node;           // optional `recap:` block (snapshot service)
    std::string base_dir;             // for resolving relative schema paths
    uint32_t    heartbeat_timeout_ms = 2000;
    uint64_t    dup_window = 1024;
    // lvc: on|off. Off for partitions whose values are built up from many
    // messages (books, accumulated volume): those come from recap instead, and a
    // last value of one update would mislead.
    bool        lvc = true;

    // Parses one entry of the `partitions:` sequence. `id` is the index.
    static bool parse(const YAML::Node& node, uint16_t id, const std::string& base_dir,
                      PartitionConfig& out, std::string& err);
};

enum class PartitionState : uint8_t { INIT = 0, ACTIVE = 1, DEGRADED = 2 };
enum class DecoderType : uint8_t { SBE_ROUTING = 0, DBN = 1, FLATBUFFERS = 2 };
const char* decoder_type_name(DecoderType t);
const char* partition_state_name(PartitionState s);

class Partition {
public:
    static constexpr uint32_t kMaxSessions = 256;

    // Cold. Loads the schema, builds decoder + framer, constructs the transport
    // via the registry (or uses `transport_override` when given, for tests).
    bool build(const PartitionConfig& cfg, std::string& err,
               std::unique_ptr<Transport> transport_override = nullptr,
               std::unique_ptr<RecapTransport> recap_override = nullptr);
    bool open(std::string& err);   // opens the transport
    void close() noexcept;

    // Cold accessors.
    const std::string& name() const noexcept { return cfg_.name; }
    uint16_t id() const noexcept { return cfg_.id; }
    uint64_t heartbeat_timeout_ns() const noexcept { return static_cast<uint64_t>(cfg_.heartbeat_timeout_ms) * 1'000'000ull; }
    DecoderType decoder_type() const noexcept { return decoder_type_; }
    // SBE only: nullptr for other decoders.
    const SbeSchema* schema() const noexcept { return decoder_type_ == DecoderType::SBE_ROUTING ? &schema_ : nullptr; }
    const SbeRoutingDecoder& sbe_decoder() const noexcept { return sbe_decoder_; }
    const DbnDecoder& dbn_decoder() const noexcept { return dbn_decoder_; }
    const FlatbuffersDecoder& flatbuffers_decoder() const noexcept { return fb_decoder_; }
    const std::vector<std::string>& decoder_warnings() const noexcept {
        switch (decoder_type_) {
            case DecoderType::DBN:         return dbn_decoder_.warnings();
            case DecoderType::FLATBUFFERS: return fb_decoder_.warnings();
            default:                       return sbe_decoder_.warnings();
        }
    }
    const Framer& framer() const noexcept { return framer_; }
    // True when symbol_id on this partition is the raw instrument id, not a table id.
    bool symbol_passthrough() const noexcept {
        switch (decoder_type_) {
            case DecoderType::DBN:         return dbn_decoder_.symbol_passthrough();
            case DecoderType::FLATBUFFERS: return fb_decoder_.symbol_passthrough();
            default:                       return sbe_decoder_.symbol_passthrough();
        }
    }

    // True when integer symbols on this partition are interned per venue (symbol_scope: venue).
    bool symbol_venue_scope() const noexcept {
        switch (decoder_type_) {
            case DecoderType::DBN:         return dbn_decoder_.symbol_venue_scope();
            case DecoderType::FLATBUFFERS: return fb_decoder_.symbol_venue_scope();
            default:                       return sbe_decoder_.symbol_venue_scope();
        }
    }

    // Hot. Dispatches to the configured decoder with a branch, not a virtual
    // call (INV: one indirect call per packet batch, which the transport uses).
    DecodeResult decode(const uint8_t* msg, uint32_t avail, TickHeader& out, SymbolResolver& syms) const noexcept {
        switch (decoder_type_) {
            case DecoderType::DBN:         return dbn_decoder_.decode(msg, avail, out, syms);
            case DecoderType::FLATBUFFERS: return fb_decoder_.decode(msg, avail, out, syms);
            default:                       return sbe_decoder_.decode(msg, avail, out, syms);
        }
    }
    // Cold (recap manager thread). Reads a recap marker's fields; false when
    // the message is not a mapped marker. dbn has no markers.
    bool read_recap_fields(const uint8_t* msg, uint32_t avail, RecapMarkerFields& out) const noexcept {
        switch (decoder_type_) {
            case DecoderType::DBN:         return false;
            case DecoderType::FLATBUFFERS: return fb_decoder_.read_recap_fields(msg, avail, out);
            default:                       return sbe_decoder_.read_recap_fields(msg, avail, out);
        }
    }
    // Recap (cold). has_recap() when the partition declares a snapshot service;
    // the recap manager takes the component at agent start.
    bool has_recap() const noexcept { return has_recap_; }
    const RecapConfig& recap_config() const noexcept { return recap_cfg_; }
    std::unique_ptr<RecapTransport> take_recap_transport() noexcept { return std::move(recap_transport_); }
    const RecapTransport* recap_transport() const noexcept { return recap_transport_.get(); } // null once taken

    Transport& transport() noexcept { return *transport_; }
    const Transport& transport() const noexcept { return *transport_; }
    bool has_transport() const noexcept { return transport_ != nullptr; }
    const PartitionConfig& config() const noexcept { return cfg_; }

    // Hot state (pipeline thread only, except counters which are read cold).
    PartitionCounters counters;
    RecapCounters     recap_counters;        // written by the recap manager thread
    uint64_t          expected_seq = 0;      // 0 = unknown, resync on next
    // Backward jumps smaller than this are duplicates; larger ones mean the
    // publisher restarted its sequence and we resync (counted as seq_resets).
    uint64_t          dup_window = 1024;
    bool              lvc_enabled = true;    // from config `lvc`
    uint64_t          last_seq = 0;
    uint64_t          last_rx_ns = 0;
    PartitionState    state = PartitionState::INIT;
    Session*          sessions[kMaxSessions] = {};
    uint32_t          n_sessions = 0;
    // Pending recaps and held messages; set by the owning Pipeline when the
    // partition has a recap block, nullptr otherwise.
    RecapState*       recap = nullptr;

    // Hot. Returns false if full or already attached.
    bool attach(Session* s) noexcept;
    bool detach(Session* s) noexcept;

    // Cold snapshot for /partitions.
    std::atomic<uint8_t>  state_pub{0};
    std::atomic<uint64_t> last_seq_pub{0};
    std::atomic<uint64_t> last_rx_pub{0};

private:
    PartitionConfig            cfg_;
    DecoderType                decoder_type_ = DecoderType::SBE_ROUTING;
    SbeSchema                  schema_;        // SBE only
    SbeRoutingDecoder          sbe_decoder_;   // exactly one of these is initialised
    DbnDecoder                 dbn_decoder_;
    FlatbuffersDecoder         fb_decoder_;
    Framer                     framer_;
    std::unique_ptr<Transport> transport_;
    bool                       has_recap_ = false;
    RecapConfig                recap_cfg_;
    std::unique_ptr<RecapTransport> recap_transport_;
    bool build_recap(std::unique_ptr<RecapTransport> recap_override, std::string& err);
};

} // namespace subplane
