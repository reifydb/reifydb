// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{BorrowedSpan, DateTime, Error, Span};
use crate::error::diagnostic::temporal;
use super::date::parse_date;
use super::time::parse_time;

pub fn parse_datetime(span: impl Span) -> Result<DateTime, Error> {
    let parts = span.split('T');
    if parts.len() != 2 {
        return Err(Error(temporal::invalid_datetime_format(span.to_owned())));
    }

    let date_span = BorrowedSpan::new(parts[0].fragment());
    let time_span = BorrowedSpan::new(parts[1].fragment());
    let date = parse_date(date_span)?;
    let time = parse_time(time_span)?;

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
    use super::parse_datetime;
    use crate::OwnedSpan;

    #[test]
    fn test_basic() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_timezone_z() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00Z");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_milliseconds() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00.123Z");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123000000Z");
    }

    #[test]
    fn test_with_microseconds() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00.123456Z");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456000Z");
    }

    #[test]
    fn test_with_nanoseconds() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00.123456789Z");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456789Z");
    }

    #[test]
    fn test_leap_year() {
        let span = OwnedSpan::testing("2024-02-29T00:00:00");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-02-29T00:00:00.000000000Z");
    }

    #[test]
    fn test_boundaries() {
        let span = OwnedSpan::testing("2000-01-01T00:00:00");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2000-01-01T00:00:00.000000000Z");

        let span = OwnedSpan::testing("2024-12-31T23:59:59");
        let datetime = parse_datetime(span).unwrap();
        assert_eq!(datetime.to_string(), "2024-12-31T23:59:59.000000000Z");
    }

    #[test]
    fn test_invalid_format() {
        let span = OwnedSpan::testing("2024-03-15");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_002");
    }

    #[test]
    fn test_invalid_date_format() {
        let span = OwnedSpan::testing("2024-03T14:30:00");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_001");
    }

    #[test]
    fn test_invalid_time_format() {
        let span = OwnedSpan::testing("2024-03-15T14:30");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_003");
    }

    #[test]
    fn test_invalid_year() {
        let span = OwnedSpan::testing("invalid-03-15T14:30:00");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_005");
    }

    #[test]
    fn test_invalid_date_values() {
        let span = OwnedSpan::testing("2024-13-32T23:30:40");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_012");
    }

    #[test]
    fn test_invalid_time_value() {
        let span = OwnedSpan::testing("2024-09-09T30:70:80");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_013");
    }

    #[test]
    fn test_invalid_fractional_seconds() {
        let span = OwnedSpan::testing("2024-03-15T14:30:00.123.456");
        let err = parse_datetime(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_011");
    }
}