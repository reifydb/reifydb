// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::Error;
use crate::evaluate::constant::date::parse_date;
use crate::evaluate::constant::time::parse_time;
use reifydb_core::{DateTime, Span};
use reifydb_diagnostic::temporal;

pub(crate) fn parse_datetime(span: &Span) -> evaluate::Result<DateTime> {
    let parts = span.split('T');
    if parts.len() != 2 {
        return Err(Error(temporal::invalid_datetime_format(span.clone())));
    }

    let date = parse_date(&parts[0])?;
    let time = parse_time(&parts[1])?;

    Ok(DateTime::new(
        date.year(),
        date.month(),
        date.day(),
        time.hour(),
        time.minute(),
        time.second(),
        time.nanosecond(),
    )
    .unwrap()) // safe because date and time already checked
}

#[cfg(test)]
mod tests {
    use crate::evaluate::constant::datetime::parse_datetime;
    use reifydb_core::Span;

    #[test]
    fn test_basic() {
        let span = Span::testing("2024-03-15T14:30:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_timezone_z() {
        let span = Span::testing("2024-03-15T14:30:00Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_milliseconds() {
        let span = Span::testing("2024-03-15T14:30:00.123Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123000000Z");
    }

    #[test]
    fn test_with_microseconds() {
        let span = Span::testing("2024-03-15T14:30:00.123456Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456000Z");
    }

    #[test]
    fn test_with_nanoseconds() {
        let span = Span::testing("2024-03-15T14:30:00.123456789Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456789Z");
    }

    #[test]
    fn test_leap_year() {
        let span = Span::testing("2024-02-29T00:00:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-02-29T00:00:00.000000000Z");
    }

    #[test]
    fn test_boundaries() {
        let span = Span::testing("2000-01-01T00:00:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2000-01-01T00:00:00.000000000Z");

        let span = Span::testing("2024-12-31T23:59:59");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-12-31T23:59:59.000000000Z");
    }

    #[test]
    fn test_invalid_format() {
        let span = Span::testing("2024-03-15");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_002");
    }

    #[test]
    fn test_invalid_date_format() {
        let span = Span::testing("2024-03T14:30:00");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_001");
    }

    #[test]
    fn test_invalid_time_format() {
        let span = Span::testing("2024-03-15T14:30");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_003");
    }

    #[test]
    fn test_invalid_year() {
        let span = Span::testing("invalid-03-15T14:30:00");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_005");
    }

    #[test]
    fn test_invalid_date_values() {
        let span = Span::testing("2024-13-32T23:30:40");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_012");
    }

    #[test]
    fn test_invalid_time_value() {
        let span = Span::testing("2024-09-09T30:70:80");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_013");
    }

    #[test]
    fn test_invalid_fractional_seconds() {
        let span = Span::testing("2024-03-15T14:30:00.123.456");
        let err = parse_datetime(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_011");
    }
}
