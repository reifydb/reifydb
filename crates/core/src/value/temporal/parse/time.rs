// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::error::diagnostic::temporal;
use crate::{BorrowedSpan, Error, Span, Time, return_error};

pub fn parse_time(span: impl Span) -> Result<Time, Error> {
    // Parse time in format HH:MM:SS[.sss[sss[sss]]][Z]

    let fragment = span.fragment();
    let mut time_str = fragment;

    if time_str.ends_with('Z') {
        time_str = &time_str[..time_str.len() - 1];
    }

    // Create a new span with the trimmed fragment but preserve original position
    let time_span = BorrowedSpan { column: span.column(), line: span.line(), fragment: time_str };
    let time_span_parts = time_span.split(':');

    if time_span_parts.len() != 3 {
        return_error!(temporal::invalid_time_format(time_span));
    }

    // Check for empty time parts
    if time_span_parts[0].trimmed_fragment().is_empty() {
        return_error!(temporal::empty_time_component(time_span_parts[0].to_owned()));
    }
    if time_span_parts[1].trimmed_fragment().is_empty() {
        return_error!(temporal::empty_time_component(time_span_parts[1].to_owned()));
    }
    if time_span_parts[2].trimmed_fragment().is_empty() {
        return_error!(temporal::empty_time_component(time_span_parts[2].to_owned()));
    }

    let hour = time_span_parts[0]
        .trimmed_fragment()
        .parse::<u32>()
        .map_err(|_| Error(temporal::invalid_hour(time_span_parts[0].to_owned())))?;

    let minute = time_span_parts[1]
        .trimmed_fragment()
        .parse::<u32>()
        .map_err(|_| Error(temporal::invalid_minute(time_span_parts[1].to_owned())))?;

    // Parse seconds and optional fractional seconds
    let seconds_with_fraction = time_span_parts[2].trimmed_fragment();
    let (second, nanosecond) = if seconds_with_fraction.contains('.') {
        let second_parts: Vec<&str> = seconds_with_fraction.split('.').collect();
        if second_parts.len() != 2 {
            return_error!(temporal::invalid_fractional_seconds(time_span_parts[2].to_owned()));
        }

        let second = second_parts[0]
            .parse::<u32>()
            .map_err(|_| Error(temporal::invalid_second(time_span_parts[2].to_owned())))?;
        let fraction_str = second_parts[1];

        // Pad or truncate fractional seconds to 9 digits (nanoseconds)
        let padded_fraction = if fraction_str.len() < 9 {
            format!("{:0<9}", fraction_str)
        } else {
            fraction_str[..9].to_string()
        };

        let nanosecond = padded_fraction.parse::<u32>().map_err(|_| {
            Error(temporal::invalid_fractional_seconds(time_span_parts[2].to_owned()))
        })?;
        (second, nanosecond)
    } else {
        let second = seconds_with_fraction
            .parse::<u32>()
            .map_err(|_| Error(temporal::invalid_second(time_span_parts[2].to_owned())))?;
        (second, 0)
    };

    Time::new(hour, minute, second, nanosecond)
        .ok_or_else(|| Error(temporal::invalid_time_values(span.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::parse_time;
    use crate::OwnedSpan;

    #[test]
    fn test_basic() {
        let span = OwnedSpan::testing("14:30:00");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_with_timezone_z() {
        let span = OwnedSpan::testing("14:30:00Z");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_with_milliseconds() {
        let span = OwnedSpan::testing("14:30:00.123");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123000000");
    }

    #[test]
    fn test_with_microseconds() {
        let span = OwnedSpan::testing("14:30:00.123456");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123456000");
    }

    #[test]
    fn test_with_nanoseconds() {
        let span = OwnedSpan::testing("14:30:00.123456789");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123456789");
    }

    #[test]
    fn test_with_utc_timezone() {
        let span = OwnedSpan::testing("14:30:00Z");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_boundaries() {
        let span = OwnedSpan::testing("00:00:00");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "00:00:00.000000000");

        let span = OwnedSpan::testing("23:59:59");
        let time = parse_time(span).unwrap();
        assert_eq!(time.to_string(), "23:59:59.000000000");
    }

    #[test]
    fn test_invalid_format() {
        let span = OwnedSpan::testing("14:30");
        let err = parse_time(span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_003");
    }

    #[test]
    fn test_invalid_hour() {
        let span = OwnedSpan::testing("invalid:30:00");
        let result = parse_time(span);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_008");
    }

    #[test]
    fn test_invalid_minute() {
        let span = OwnedSpan::testing("14:invalid:00");
        let result = parse_time(span);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_009");
    }

    #[test]
    fn test_invalid_second() {
        let span = OwnedSpan::testing("14:30:invalid");
        let result = parse_time(span);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_010");
    }

    #[test]
    fn test_invalid_time_values() {
        let span = OwnedSpan::testing("25:70:80");
        let result = parse_time(span);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_013");
    }

    #[test]
    fn test_invalid_fractional_seconds() {
        let span = OwnedSpan::testing("14:30:00.123.456");
        let result = parse_time(span);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_011");
    }
}
