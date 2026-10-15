#include "subplane/recap/recap_manager.h"
#include "subplane/recap/recap_transport.h"

namespace subplane {

const char* recap_send_status_name(RecapSendStatus s) noexcept {
    switch (s) {
        case RecapSendStatus::OK:             return "ok";
        case RecapSendStatus::BACK_PRESSURED: return "back_pressured";
        case RecapSendStatus::NOT_CONNECTED:  return "not_connected";
        default:                              return "failed";
    }
}

const char* recap_status_name(RecapStatus s) noexcept {
    switch (s) {
        case RecapStatus::OK:          return "ok";
        case RecapStatus::REJECTED:    return "rejected";
        case RecapStatus::TIMEOUT:     return "timeout";
        case RecapStatus::OVERFLOW:    return "overflow";
        default:                       return "send_failed";
    }
}

} // namespace subplane
