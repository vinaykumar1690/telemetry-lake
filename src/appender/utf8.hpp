#ifndef APPENDER_UTF8_HPP
#define APPENDER_UTF8_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace appender::utf8 {

// Length of the valid UTF-8 sequence starting at s[i], or 0 if invalid
// (overlong forms, surrogates and code points above U+10FFFF are invalid)
inline std::size_t sequenceLength(std::string_view s, std::size_t i) {
    const auto byte = [&](std::size_t k) { return static_cast<std::uint8_t>(s[k]); };
    const std::uint8_t b0 = byte(i);
    if (b0 < 0x80) return 1;
    auto continuation = [&](std::size_t k) { return k < s.size() && (byte(k) & 0xC0) == 0x80; };
    if (b0 >= 0xC2 && b0 <= 0xDF) return continuation(i + 1) ? 2 : 0;
    if (b0 >= 0xE0 && b0 <= 0xEF) {
        if (!continuation(i + 1) || !continuation(i + 2)) return 0;
        const std::uint8_t b1 = byte(i + 1);
        if (b0 == 0xE0 && b1 < 0xA0) return 0;   // overlong
        if (b0 == 0xED && b1 >= 0xA0) return 0;  // surrogate
        return 3;
    }
    if (b0 >= 0xF0 && b0 <= 0xF4) {
        if (!continuation(i + 1) || !continuation(i + 2) || !continuation(i + 3)) return 0;
        const std::uint8_t b1 = byte(i + 1);
        if (b0 == 0xF0 && b1 < 0x90) return 0;   // overlong
        if (b0 == 0xF4 && b1 >= 0x90) return 0;  // above U+10FFFF
        return 4;
    }
    return 0;
}

inline bool isValid(std::string_view s) {
    for (std::size_t i = 0; i < s.size();) {
        const auto n = sequenceLength(s, i);
        if (n == 0) return false;
        i += n;
    }
    return true;
}

// Replaces each invalid byte with U+FFFD so one bad string cannot fail a batch
inline std::string sanitize(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size();) {
        if (const auto n = sequenceLength(s, i); n > 0) {
            out.append(s.substr(i, n));
            i += n;
        } else {
            out += "\xEF\xBF\xBD";
            ++i;
        }
    }
    return out;
}

}  // namespace appender::utf8

#endif  // APPENDER_UTF8_HPP
