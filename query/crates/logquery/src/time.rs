//! Time expressions accepted by the frontends.

use chrono::DateTime;

use crate::error::{QueryError, Result};

/// Parses a point in time into microseconds since the epoch:
/// - RFC 3339 (`2026-10-18T10:00:00Z`, `2026-10-18T10:00:00.5+02:00`)
/// - `now`, or `now-<n><unit>` with unit `s`, `m`, `h`, `d`, `w`
/// - an integer: milliseconds since the epoch
pub fn parse_instant(text: &str, now_us: i64) -> Result<i64> {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("now") {
        if rest.is_empty() {
            return Ok(now_us);
        }
        let ago = rest
            .strip_prefix('-')
            .ok_or_else(|| QueryError::new(format!("invalid time '{text}': expected now-<duration>")))?;
        return Ok(now_us.saturating_sub(parse_duration_us(ago)?));
    }
    if let Ok(millis) = text.parse::<i64>() {
        return millis.checked_mul(1000).ok_or_else(|| QueryError::new(format!("time '{text}' is out of range")));
    }
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_micros())
        .map_err(|e| QueryError::new(format!("invalid time '{text}': {e}")))
}

/// Parses `<n><unit>` (`15m`, `2h`, `7d`) into microseconds
pub fn parse_duration_us(text: &str) -> Result<i64> {
    let invalid = || QueryError::new(format!("invalid duration '{text}': expected e.g. 15m, 2h, 7d"));
    let split = text.find(|c: char| !c.is_ascii_digit()).ok_or_else(invalid)?;
    let (number, unit) = text.split_at(split);
    let n: i64 = number.parse().map_err(|_| invalid())?;
    let unit_us: i64 = match unit {
        "s" => 1_000_000,
        "m" => 60 * 1_000_000,
        "h" => 3600 * 1_000_000,
        "d" => 86_400 * 1_000_000,
        "w" => 7 * 86_400 * 1_000_000,
        _ => return Err(invalid()),
    };
    n.checked_mul(unit_us).ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_792_000_000_000_000;

    #[test]
    fn relative_and_absolute() {
        assert_eq!(parse_instant("now", NOW).unwrap(), NOW);
        assert_eq!(parse_instant("now-15m", NOW).unwrap(), NOW - 900_000_000);
        assert_eq!(parse_instant(" now-2d ", NOW).unwrap(), NOW - 2 * 86_400_000_000);
        assert_eq!(parse_instant("2026-10-18T00:00:00Z", NOW).unwrap(), 1_792_281_600_000_000);
        assert_eq!(parse_instant("2026-10-18T02:00:00.5+02:00", NOW).unwrap(), 1_792_281_600_500_000);
        assert_eq!(parse_instant("1792281600000", NOW).unwrap(), 1_792_281_600_000_000);
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["now+1h", "now-", "now-5y", "now-h", "yesterday", "2026-10-18", "99999999999999999999"] {
            assert!(parse_instant(bad, NOW).is_err(), "{bad}");
        }
    }
}
