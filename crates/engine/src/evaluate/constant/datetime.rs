// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use reifydb_core::{DateTime, Span};

pub(crate) fn parse_datetime(span: &Span) -> evaluate::Result<DateTime> {
    let fragment = &span.fragment;
    // Parse datetime in format YYYY-MM-DDTHH:MM:SS[.sss[sss[sss]]][Z|±HH:MM]
    let parts: Vec<&str> = fragment.split('T').collect();
    if parts.len() != 2 {
        panic!("Invalid datetime format");
    }

    let date_part = parts[0];
    let mut time_part = parts[1].to_string();

    // Parse date part
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        panic!("Invalid date format in datetime");
    }

    let year = date_parts[0].parse::<i32>().unwrap_or_else(|_| panic!("Invalid year"));
    let month = date_parts[1].parse::<u32>().unwrap_or_else(|_| panic!("Invalid month"));
    let day = date_parts[2].parse::<u32>().unwrap_or_else(|_| panic!("Invalid day"));

    // Remove timezone indicator if present
    if time_part.ends_with('Z') {
        time_part = time_part[..time_part.len() - 1].to_string();
    } else if time_part.contains('+') || time_part.rfind('-').map_or(false, |pos| pos > 0) {
        // Find timezone offset (+ or - that's not at the start)
        let tz_pos = time_part.find('+').or_else(|| time_part.rfind('-').filter(|&pos| pos > 0));
        if let Some(pos) = tz_pos {
            time_part = time_part[..pos].to_string();
        }
    }

    // Parse time part (HH:MM:SS[.sss[sss[sss]]])
    let time_parts: Vec<&str> = time_part.split(':').collect();
    if time_parts.len() != 3 {
        panic!("Invalid time format in datetime");
    }

    let hour = time_parts[0].parse::<u32>().unwrap_or_else(|_| panic!("Invalid hour"));
    let minute = time_parts[1].parse::<u32>().unwrap_or_else(|_| panic!("Invalid minute"));

    // Parse seconds and optional fractional seconds
    let seconds_with_fraction = time_parts[2];
    let (second, nanosecond) = if seconds_with_fraction.contains('.') {
        let second_parts: Vec<&str> = seconds_with_fraction.split('.').collect();
        if second_parts.len() != 2 {
            panic!("Invalid fractional seconds format");
        }

        let second = second_parts[0].parse::<u32>().unwrap_or_else(|_| panic!("Invalid second"));
        let fraction_str = second_parts[1];

        // Pad or truncate fractional seconds to 9 digits (nanoseconds)
        let padded_fraction = if fraction_str.len() < 9 {
            format!("{:0<9}", fraction_str)
        } else {
            fraction_str[..9].to_string()
        };

        let nanosecond =
            padded_fraction.parse::<u32>().unwrap_or_else(|_| panic!("Invalid fractional seconds"));
        (second, nanosecond)
    } else {
        let second =
            seconds_with_fraction.parse::<u32>().unwrap_or_else(|_| panic!("Invalid second"));
        (second, 0)
    };

    Ok(DateTime::new(year, month, day, hour, minute, second, nanosecond)
        .unwrap_or_else(|| panic!("Invalid datetime")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::{SpanColumn, SpanLine};

    fn make_span(value: &str) -> Span {
        Span { column: SpanColumn(0), line: SpanLine(1), fragment: value.to_string() }
    }

    #[test]
    fn test_basic() {
        let span = make_span("2024-03-15T14:30:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_timezone_z() {
        let span = make_span("2024-03-15T14:30:00Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_milliseconds() {
        let span = make_span("2024-03-15T14:30:00.123Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123000000Z");
    }

    #[test]
    fn test_with_microseconds() {
        let span = make_span("2024-03-15T14:30:00.123456Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456000Z");
    }

    #[test]
    fn test_with_nanoseconds() {
        let span = make_span("2024-03-15T14:30:00.123456789Z");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.123456789Z");
    }

    #[test]
    fn test_with_positive_timezone() {
        let span = make_span("2024-03-15T14:30:00+05:30");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_with_negative_timezone() {
        let span = make_span("2024-03-15T14:30:00-05:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-03-15T14:30:00.000000000Z");
    }

    #[test]
    fn test_leap_year() {
        let span = make_span("2024-02-29T00:00:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-02-29T00:00:00.000000000Z");
    }

    #[test]
    fn test_boundaries() {
        let span = make_span("2000-01-01T00:00:00");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2000-01-01T00:00:00.000000000Z");

        let span = make_span("2024-12-31T23:59:59");
        let datetime = parse_datetime(&span).unwrap();
        assert_eq!(datetime.to_string(), "2024-12-31T23:59:59.000000000Z");
    }

    #[test]
    #[should_panic(expected = "Invalid datetime format")]
    fn test_invalid_format() {
        let span = make_span("2024-03-15");
        parse_datetime(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid date format in datetime")]
    fn test_invalid_date_format() {
        let span = make_span("2024-03T14:30:00");
        parse_datetime(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid time format in datetime")]
    fn test_invalid_time_format() {
        let span = make_span("2024-03-15T14:30");
        parse_datetime(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid year")]
    fn test_invalid_year() {
        let span = make_span("invalid-03-15T14:30:00");
        parse_datetime(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid datetime")]
    fn test_invalid_datetime_values() {
        let span = make_span("2024-13-32T25:70:80");
        parse_datetime(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid fractional seconds format")]
    fn test_invalid_fractional_seconds() {
        let span = make_span("2024-03-15T14:30:00.123.456");
        parse_datetime(&span).unwrap();
    }
}
