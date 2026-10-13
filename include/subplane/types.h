#pragma once

// Cold-path string/time aliases used by config, logging, metrics and the HTTP API.
// Hot-path types live in core/message.h.

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "subplane/core/message.h"

namespace subplane {

using StringType  = std::string;
using PathType    = std::string;
using ConfigType  = std::string;

// Logging
using LogLevelType      = std::string;
using ComponentNameType = std::string;
using EventNameType     = std::string;
using LogMessageType    = std::string;
using ThreadIdType      = std::string;

// Metrics
using OperationNameType = std::string;
using MetricNameType    = std::string;

// Misc
using MetadataType  = std::unordered_map<StringType, StringType>;
using TimestampType = std::chrono::system_clock::time_point;
using DurationType  = std::chrono::milliseconds;
using SizeType      = size_t;
using CountType     = uint64_t;

} // namespace subplane
