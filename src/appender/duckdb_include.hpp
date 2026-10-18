#ifndef APPENDER_DUCKDB_INCLUDE_HPP
#define APPENDER_DUCKDB_INCLUDE_HPP

// Include DuckDB through this header, never duckdb.hpp directly.
//
// DuckDB 1.5.x headers construct a unique_ptr<ActiveTimer> inline
// (QueryMetrics in duckdb/main/profiling_utils.hpp) before ActiveTimer is
// defined. C++23 made unique_ptr's destructor constexpr, so it is
// instantiated right there and fails on the incomplete type. Declaring the
// deleter up front and defining it out of line (duckdb_session.cpp, where the
// type is complete) avoids the eager instantiation; it still just deletes.

#include <memory>

namespace duckdb {
struct ActiveTimer;
}  // namespace duckdb

template <>
struct std::default_delete<duckdb::ActiveTimer> {
    constexpr default_delete() noexcept = default;
    void operator()(duckdb::ActiveTimer* timer) const noexcept;
};

#include "duckdb.hpp"

#endif  // APPENDER_DUCKDB_INCLUDE_HPP
