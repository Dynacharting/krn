#include "krn/util/Config.hpp"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <sstream>
#include <algorithm>

#include "krn/engine/ConfigNamespaceRegistry.hpp"
#include "krn/util/Logger.hpp"

#ifdef _WIN32
  #define NOMINMAX
  #include <windows.h>
#endif

namespace krn {
namespace util {

std::string Config::trim(const std::string& s) {
  const auto is_ws = [](unsigned char c){ return std::isspace(c) != 0; };
  auto b = std::find_if_not(s.begin(), s.end(), is_ws);
  auto e = std::find_if_not(s.rbegin(), s.rend(), is_ws).base();
  if (b >= e) return {};
  return std::string(b, e);
}

bool Config::parseLineKV(const std::string& line, std::string& k, std::string& v) {
  auto pos = line.find('=');
  if (pos == std::string::npos) return false;
  k = trim(line.substr(0, pos));
  v = trim(line.substr(pos + 1));
  if (k.empty()) return false;
  return true;
}

bool Config::loadFromFile(const std::string& path) {
  // Simple INI-ish parser: key=value per line, '#' or ';' start comments.
  // Unknown keys are parked, not rejected, so an older build still starts;
  // reportUnclaimedKeys() warns about the ones nobody claims (ENC-1595).

  FILE* f = nullptr;

#ifdef _WIN32
  if (fopen_s(&f, path.c_str(), "rb") != 0 || !f) {
    return false;
  }
#else
  f = std::fopen(path.c_str(), "rb");
  if (!f) return false;
#endif

  auto closeFile = [](FILE* fp) { if (fp) std::fclose(fp); };
  std::unique_ptr<FILE, decltype(closeFile)> fileGuard(f, closeFile);

  std::string line;
  line.reserve(1024);

  // Read whole file
  while (true) {
    char tmp[1024];
    if (!std::fgets(tmp, sizeof(tmp), f)) break;
    line.assign(tmp);

    // Detect truncated lines (fgets fills the buffer without a newline).
    // Discard the rest of the line to avoid silent data corruption.
    if (line.size() == sizeof(tmp) - 1 && !line.empty() && line.back() != '\n') {
      // Consume remainder of the oversized line
      while (std::fgets(tmp, sizeof(tmp), f)) {
        if (std::strchr(tmp, '\n')) break;
      }
      continue; // skip this line entirely
    }

    // Strip CR/LF
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) line.pop_back();

    auto s = trim(line);
    if (s.empty()) continue;
    if (s[0] == '#' || s[0] == ';') continue; // comment

    std::string key, val;
    if (!parseLineKV(s, key, val)) continue;

    // NOTE: std::atoi returns 0 for non-numeric input. Port/size values have
    // explicit range checks, so invalid input produces safe defaults rather than UB.
    if      (key == "wsPort")        { int p = std::atoi(val.c_str()); if (p > 0 && p <= 65535) wsPort = p; }
    else if (key == "threadPoolSize") { int v = std::atoi(val.c_str()); if (v >= 0) threadPoolSize = v; }
    else if (key == "taHistoryMax") { int v = std::atoi(val.c_str()); if (v > 0) taHistoryMax = v; }
    else if (key == "maxSymbols") { int v = std::atoi(val.c_str()); if (v > 0) maxSymbols = v; }
    else if (key == "maxFieldsPerSymbol") { int v = std::atoi(val.c_str()); if (v > 0) maxFieldsPerSymbol = v; }
    else if (key == "atomicKeyNamespaceByField") { atomicKeyNamespaceByField = (val == "true" || val == "1" || val == "yes"); }
    else if (key == "inlineDagDrive") { inlineDagDrive = (val == "true" || val == "1" || val == "yes"); }
    // Canonical ingress entries: ingress.N.kind = ..., ingress.N.<param> = ...
    else if (key.size() > 8 && key.substr(0, 8) == "ingress.") {
      auto dot2 = key.find('.', 8);
      if (dot2 != std::string::npos) {
        std::string idxStr = key.substr(8, dot2 - 8);
        if (!idxStr.empty() && std::isdigit(static_cast<unsigned char>(idxStr[0]))) {
          int idx = std::atoi(idxStr.c_str());
          if (idx >= 0 && idx < 64) {
            while (static_cast<int>(ingress.size()) <= idx) ingress.emplace_back();
            std::string field = key.substr(dot2 + 1);
            if (field == "kind") {
              ingress[idx].kind = val;
            } else {
              ingress[idx].params[field] = val;
            }
          }
        }
      }
    }
    else if (key == "metricsEnabled") { metricsEnabled = (val == "true" || val == "1" || val == "yes"); }
    else if (key == "metricsIntervalSec") { int v = std::atoi(val.c_str()); if (v > 0) metricsIntervalSec = v; }
    else if (key == "logLevel") { logLevel = val; }
    else if (key == "forumUrl") { forumUrl = val; }
    else if (key == "forumTenantId") { forumTenantId = val; }
    else if (key == "forumAuthToken") { forumAuthToken = val; }
    else {
      // Unknown to the engine — park for later replay through
      // ConfigNamespaceRegistry once connectors have registered their
      // readers. See dispatchPendingKeys().
      _pendingKeys.push_back({std::move(key), std::move(val), path});
    }
  }

  // fileGuard closes f automatically via RAII.

  return true;
}

std::size_t Config::dispatchPendingKeys() {
  std::size_t consumed = 0;
  for (auto& p : _pendingKeys) {
    if (p.dispatched) continue;
    p.dispatched = true;
    if (krn::engine::ConfigNamespaceRegistry::dispatch(p.key, p.value)) {
      p.claimed = true;
      ++consumed;
    }
  }
  return consumed;
}

void Config::markConsumed(std::string_view key) {
  _consumed.emplace(key);
}

std::vector<Config::UnclaimedKey> Config::unclaimedKeys() const {
  std::vector<UnclaimedKey> out;
  std::unordered_set<std::string> seen;
  for (const auto& p : _pendingKeys) {
    if (p.claimed || _consumed.count(p.key) > 0) continue;
    if (!seen.insert(p.key).second) continue;
    out.push_back({p.key, p.file});
  }
  return out;
}

std::size_t Config::reportUnclaimedKeys() const {
  const auto keys = unclaimedKeys();
  for (const auto& u : keys) {
    logger().log(LogLevel::Warn, "config.unclaimed_key",
                 {{"key", u.key},
                  {"file", u.file},
                  {"detail", "no engine field or plugin claimed it; ignored"}});
  }
  return keys.size();
}

} // namespace util
} // namespace krn
