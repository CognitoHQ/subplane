#include <doctest.h>

#include "flashtick/core/message.h"
#include "flashtick/shm/control_events.h"

using namespace flashtick;

TEST_CASE("TickHeader is a frozen 64-byte ABI") {
    CHECK(sizeof(TickHeader) == 64);
    CHECK(alignof(TickHeader) == 64);
    CHECK(std::is_trivially_copyable_v<TickHeader>);
    TickHeader h{};
    CHECK(h.seq_tag == 0);
    CHECK(h.kind == static_cast<uint8_t>(Kind::DATA));
}

TEST_CASE("make_control_header classifies bus admin vs agent control") {
    auto bus = make_control_header(ControlCode::BUS_HEARTBEAT, 3, 123, 0);
    CHECK(bus.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    CHECK(bus.partition_id == 3);
    CHECK(bus.ingest_ts == 123);
    CHECK(is_control(bus));
    CHECK_FALSE(is_data(bus));

    auto gap = make_control_header(ControlCode::GAP_DETECTED, 1, 5, sizeof(GapEvent));
    CHECK(gap.kind == static_cast<uint8_t>(Kind::AGENT_CONTROL));
    CHECK(gap.payload_len == sizeof(GapEvent));

    auto pad = make_control_header(ControlCode::PAD, 0, 1, 0);
    CHECK(pad.kind == static_cast<uint8_t>(Kind::PAD));
}

TEST_CASE("control event payloads are packed") {
    CHECK(sizeof(GapEvent) == 32);
    CHECK(sizeof(SymbolMappedEvent) == 32);
    CHECK(sizeof(AgentRestartedEvent) == 16);
}
