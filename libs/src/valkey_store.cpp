#include "subplane/lib/valkey_store.h"

#include "subplane/lib/runner.h"

#include <valkey/valkey.h>

#include <array>
#include <string>
#include <sys/time.h>
#include <vector>

namespace subplane::lib {

namespace {

constexpr uint64_t kNsPerMs = 1'000'000;
constexpr uint32_t kUsPerMs = 1000;

// INTENT: the take script is gcra.h in Lua; change both together. Time comes
// from the server (TIME), so clients on different hosts share one clock. It is
// counted in µs from 2026-01-01 rather than 1970: Lua numbers are doubles, and
// a smaller base keeps sub-µs precision in the stored arrival times.
constexpr const char* kTakeScript = R"lua(
local t = redis.call('TIME')
local now = (tonumber(t[1]) - 1767225600) * 1000000 + tonumber(t[2])
local want = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
if min < 1 then min = 1 end
local tat, iv, tau = {}, {}, {}
local grant = want
for i = 1, #KEYS do
  iv[i] = 1000000 / tonumber(ARGV[1 + 2 * i])
  tau[i] = tonumber(ARGV[2 + 2 * i]) * iv[i]
  local v = redis.call('GET', KEYS[i])
  v = v and tonumber(v) or now
  if v < now then v = now end
  tat[i] = v
  local a = math.floor((now + tau[i] - v) / iv[i])
  if a < grant then grant = a end
end
if grant < min then
  local w = 0
  for i = 1, #KEYS do
    local x = tat[i] + min * iv[i] - tau[i] - now
    if x > w then w = x end
  end
  return {0, math.ceil(w)}
end
for i = 1, #KEYS do
  local nt = tat[i] + grant * iv[i]
  redis.call('SET', KEYS[i], string.format('%.17g', nt), 'PX', math.ceil((nt - now) / 1000) + 1)
end
return {grant, 0}
)lua";

// KEYS: the group's lease hash, its epoch counter. ARGV: me, ttl_ms.
// The epoch counter has no TTL: an epoch is never reused.
constexpr const char* kClaimScript = R"lua(
local h = redis.call('HGET', KEYS[1], 'holder')
if not h then
  local e = redis.call('INCR', KEYS[2])
  redis.call('HSET', KEYS[1], 'holder', ARGV[1], 'epoch', e)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, e}
end
local e = tonumber(redis.call('HGET', KEYS[1], 'epoch'))
if h == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, e}
end
return {0, e}
)lua";

// KEYS: the lease hash. ARGV: me, epoch, ttl_ms.
constexpr const char* kRenewScript = R"lua(
local v = redis.call('HMGET', KEYS[1], 'holder', 'epoch')
if v[1] == ARGV[1] and v[2] == ARGV[2] then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return 1
end
return 0
)lua";

// KEYS: the lease hash. ARGV: me, epoch.
constexpr const char* kReleaseScript = R"lua(
local v = redis.call('HMGET', KEYS[1], 'holder', 'epoch')
if v[1] == ARGV[1] and v[2] == ARGV[2] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
)lua";

enum Script : size_t { TAKE, CLAIM, RENEW, RELEASE, kScripts };
constexpr std::array<const char*, kScripts> kScriptBodies = {kTakeScript, kClaimScript, kRenewScript, kReleaseScript};

timeval to_timeval(uint32_t ms) {
    timeval tv{};
    tv.tv_sec = static_cast<time_t>(ms / kUsPerMs);
    tv.tv_usec = static_cast<suseconds_t>((ms % kUsPerMs) * kUsPerMs);
    return tv;
}

struct ReplyFree {
    void operator()(valkeyReply* r) const noexcept { freeReplyObject(r); }
};
using Reply = std::unique_ptr<valkeyReply, ReplyFree>;

bool int_at(const valkeyReply* r, size_t i, long long& out) {
    if (r->type != VALKEY_REPLY_ARRAY || r->elements <= i || r->element[i]->type != VALKEY_REPLY_INTEGER) return false;
    out = r->element[i]->integer;
    return true;
}

}  // namespace

struct ValkeyStore::Impl {
    ValkeyOptions opt;
    valkeyContext* ctx = nullptr;
    uint64_t next_connect_ns = 0;
    std::string error;
    std::array<std::string, kScripts> sha;

    ~Impl() { drop(); }

    void drop() {
        if (ctx != nullptr) valkeyFree(ctx);
        ctx = nullptr;
    }

    Reply command(const std::vector<std::string>& args) {
        std::vector<const char*> argv;
        std::vector<size_t> lens;
        argv.reserve(args.size());
        lens.reserve(args.size());
        for (const std::string& a : args) {
            argv.push_back(a.data());
            lens.push_back(a.size());
        }
        Reply r(static_cast<valkeyReply*>(valkeyCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), lens.data())));
        if (!r) {
            // An I/O error or timeout: the connection is unusable.
            error = ctx->errstr;
            drop();
        }
        return r;
    }

    bool load_scripts() {
        for (size_t i = 0; i < kScripts; ++i) {
            Reply r = command({"SCRIPT", "LOAD", kScriptBodies[i]});
            if (!r) return false;
            if (r->type != VALKEY_REPLY_STRING) {
                error = r->type == VALKEY_REPLY_ERROR ? std::string(r->str, r->len) : "SCRIPT LOAD: unexpected reply";
                drop();
                return false;
            }
            sha[i].assign(r->str, r->len);
        }
        return true;
    }

    bool connected() {
        if (ctx != nullptr) return true;
        const uint64_t now = now_ns();
        if (now < next_connect_ns) return false;
        next_connect_ns = now + opt.reconnect_ms * kNsPerMs;
        const timeval ctv = to_timeval(opt.connect_timeout_ms);
        ctx = opt.unix_path.empty() ? valkeyConnectWithTimeout(opt.host.c_str(), opt.port, ctv)
                                    : valkeyConnectUnixWithTimeout(opt.unix_path.c_str(), ctv);
        if (ctx == nullptr || ctx->err != 0) {
            error = ctx != nullptr ? ctx->errstr : "valkey: cannot allocate a context";
            drop();
            return false;
        }
        if (valkeySetTimeout(ctx, to_timeval(opt.command_timeout_ms)) != VALKEY_OK) {
            error = ctx->errstr;
            drop();
            return false;
        }
        return load_scripts();
    }

    // EVALSHA, loading the scripts again if the server lost them (a restart or
    // SCRIPT FLUSH). Null on any failure.
    Reply eval(Script s, const std::vector<std::string>& keys, const std::vector<std::string>& args) {
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (!connected()) return nullptr;
            std::vector<std::string> cmd{"EVALSHA", sha[s], std::to_string(keys.size())};
            cmd.insert(cmd.end(), keys.begin(), keys.end());
            cmd.insert(cmd.end(), args.begin(), args.end());
            Reply r = command(cmd);
            if (!r) return nullptr;
            if (r->type != VALKEY_REPLY_ERROR) return r;
            error.assign(r->str, r->len);
            if (error.rfind("NOSCRIPT", 0) != 0 || !load_scripts()) return nullptr;
        }
        return nullptr;
    }

    std::string lease_key(std::string_view group) const { return opt.prefix + "elect:" + std::string(group); }
    std::string epoch_key(std::string_view group) const { return lease_key(group) + ":epoch"; }
};

ValkeyStore::ValkeyStore(ValkeyOptions opt) : impl_(std::make_unique<Impl>()) { impl_->opt = std::move(opt); }

ValkeyStore::~ValkeyStore() = default;

const std::string& ValkeyStore::last_error() const noexcept { return impl_->error; }

Take ValkeyStore::take(std::span<const Bucket> path, uint64_t want, uint64_t min) {
    std::vector<std::string> keys;
    std::vector<std::string> args{std::to_string(want), std::to_string(min)};
    for (const Bucket& b : path) {
        keys.push_back(impl_->opt.prefix + "rate:" + b.name);
        args.push_back(std::to_string(b.rate));
        args.push_back(std::to_string(b.burst));
    }
    Reply r = impl_->eval(TAKE, keys, args);
    long long granted = 0;
    long long wait = 0;
    if (!r || !int_at(r.get(), 0, granted) || !int_at(r.get(), 1, wait) || granted < 0 || wait < 0) return {};
    return {Take::OK, static_cast<uint64_t>(granted), static_cast<uint64_t>(wait)};
}

Claim ValkeyStore::claim(std::string_view group, std::string_view me, uint32_t ttl_ms) {
    Reply r = impl_->eval(CLAIM, {impl_->lease_key(group), impl_->epoch_key(group)}, {std::string(me), std::to_string(ttl_ms)});
    long long held = 0;
    long long epoch = 0;
    if (!r || !int_at(r.get(), 0, held) || !int_at(r.get(), 1, epoch) || epoch < 0) return {};
    return {held != 0 ? Claim::HELD : Claim::TAKEN, static_cast<uint64_t>(epoch)};
}

Renew ValkeyStore::renew(std::string_view group, std::string_view me, uint64_t epoch, uint32_t ttl_ms) {
    Reply r = impl_->eval(RENEW, {impl_->lease_key(group)}, {std::string(me), std::to_string(epoch), std::to_string(ttl_ms)});
    if (!r || r->type != VALKEY_REPLY_INTEGER) return Renew::ERROR;
    return r->integer != 0 ? Renew::OK : Renew::LOST;
}

void ValkeyStore::release(std::string_view group, std::string_view me, uint64_t epoch) {
    impl_->eval(RELEASE, {impl_->lease_key(group)}, {std::string(me), std::to_string(epoch)});
}

}  // namespace subplane::lib
