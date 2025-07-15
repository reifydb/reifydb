// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use reifydb_core::{Span, Time};

pub(crate) fn parse_time(span: &Span) -> evaluate::Result<Time> {
    let fragment = &span.fragment;
    // Parse time in format HH:MM:SS[.sss[sss[sss]]][Z|±HH:MM]
    let mut time_str = fragment.clone();

    // Remove timezone indicator if present
    if time_str.ends_with('Z') {
        time_str = time_str[..time_str.len() - 1].to_string();
    } else if time_str.contains('+') || time_str.rfind('-').map_or(false, |pos| pos > 0) {
        // Find timezone offset (+ or - that's not at the start)
        let tz_pos = time_str.find('+').or_else(|| time_str.rfind('-').filter(|&pos| pos > 0));
        if let Some(pos) = tz_pos {
            time_str = time_str[..pos].to_string();
        }
    }

    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() != 3 {
        panic!("Invalid time format");
    }

    let hour = parts[0].parse::<u32>().unwrap_or_else(|_| panic!("Invalid hour"));
    let minute = parts[1].parse::<u32>().unwrap_or_else(|_| panic!("Invalid minute"));

    // Parse seconds and optional fractional seconds
    let seconds_with_fraction = parts[2];
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

    Ok(Time::new(hour, minute, second, nanosecond).unwrap_or_else(|| panic!("Invalid time")))
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
        let span = make_span("14:30:00");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_with_timezone_z() {
        let span = make_span("14:30:00Z");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_with_milliseconds() {
        let span = make_span("14:30:00.123");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123000000");
    }

    #[test]
    fn test_with_microseconds() {
        let span = make_span("14:30:00.123456");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123456000");
    }

    #[test]
    fn test_with_nanoseconds() {
        let span = make_span("14:30:00.123456789");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.123456789");
    }

    #[test]
    fn test_with_positive_timezone() {
        let span = make_span("14:30:00+05:30");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_with_negative_timezone() {
        let span = make_span("14:30:00-05:00");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "14:30:00.000000000");
    }

    #[test]
    fn test_boundaries() {
        let span = make_span("00:00:00");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "00:00:00.000000000");

        let span = make_span("23:59:59");
        let time = parse_time(&span).unwrap();
        assert_eq!(time.to_string(), "23:59:59.000000000");
    }

    #[test]
    #[should_panic(expected = "Invalid time format")]
    fn test_invalid_format() {
        let span = make_span("14:30");
        parse_time(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid hour")]
    fn test_invalid_hour() {
        let span = make_span("invalid:30:00");
        parse_time(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid minute")]
    fn test_invalid_minute() {
        let span = make_span("14:invalid:00");
        parse_time(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid second")]
    fn test_invalid_second() {
        let span = make_span("14:30:invalid");
        parse_time(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid time")]
    fn test_invalid_time_values() {
        let span = make_span("25:70:80");
        parse_time(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid fractional seconds format")]
    fn test_invalid_fractional_seconds() {
        let span = make_span("14:30:00.123.456");
        parse_time(&span).unwrap();
    }
}
