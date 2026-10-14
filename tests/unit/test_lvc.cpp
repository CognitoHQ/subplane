#include <doctest.h>

#include "subplane/pipeline/lvc.h"

#include <cstring>

using namespace subplane;

TEST_CASE("lvc: status and instrument definition keep separate slots") {
    Lvc lvc;
    std::string err;
    REQUIRE(lvc.init(64, {static_cast<uint8_t>(MsgType::STATUS), static_cast<uint8_t>(MsgType::DEFINITION)}, 64, err));

    TickHeader h{};
    h.symbol_id = 5;
    h.kind = static_cast<uint8_t>(Kind::DATA);
    const uint8_t status[4] = {1, 2, 3, 4};
    const uint8_t def[8] = {'B', 'T', 'C', 'U', 'S', 'D', 'T', 0};
    h.msg_type = static_cast<uint8_t>(MsgType::DEFINITION);
    h.template_id = 10;
    lvc.update(h, def, sizeof(def));
    h.msg_type = static_cast<uint8_t>(MsgType::STATUS);
    h.template_id = 6;
    lvc.update(h, status, sizeof(status)); // used to overwrite the definition

    TickHeader out{};
    std::vector<uint8_t> payload;
    REQUIRE(lvc.read(5, static_cast<uint8_t>(MsgType::DEFINITION), out, payload));
    CHECK(out.template_id == 10);
    REQUIRE(payload.size() == sizeof(def));
    CHECK(std::memcmp(payload.data(), def, sizeof(def)) == 0);
    REQUIRE(lvc.read(5, static_cast<uint8_t>(MsgType::STATUS), out, payload));
    CHECK(out.template_id == 6);
    CHECK(payload.size() == sizeof(status));
}
