#ifndef APPENDER_SQL_HPP
#define APPENDER_SQL_HPP

#include <string>
#include <string_view>

namespace appender::sql {

// 'text' with embedded single quotes doubled. DuckDB string literals treat
// backslashes literally, so they are left alone.
inline std::string literal(std::string_view text) {
    std::string out;
    out.reserve(text.size() + 2);
    out += '\'';
    for (char c : text) {
        if (c == '\'') out += '\'';
        out += c;
    }
    out += '\'';
    return out;
}

// "name" with embedded double quotes doubled
inline std::string identifier(std::string_view name) {
    std::string out;
    out.reserve(name.size() + 2);
    out += '"';
    for (char c : name) {
        if (c == '"') out += '"';
        out += c;
    }
    out += '"';
    return out;
}

}  // namespace appender::sql

#endif  // APPENDER_SQL_HPP
