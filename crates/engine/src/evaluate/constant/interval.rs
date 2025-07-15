// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use reifydb_core::{Interval, Span};

pub(crate) fn parse_interval(span: &Span) -> evaluate::Result<Interval> {
    let fragment = &span.fragment;
    // Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)
    if !fragment.starts_with('P') {
        panic!("Invalid interval format - must start with P");
    }

    let mut chars = fragment.chars().skip(1); // Skip 'P'
    let mut total_nanos = 0i64;
    let mut current_number = String::new();
    let mut in_time_part = false;

    while let Some(c) = chars.next() {
        match c {
            'T' => {
                in_time_part = true;
            }
            '0'..='9' => {
                current_number.push(c);
            }
            'Y' => {
                if in_time_part {
                    panic!("Years not allowed in time part");
                }
                let years: i64 =
                    current_number.parse().unwrap_or_else(|_| panic!("Invalid year value"));
                total_nanos += years * 365 * 24 * 60 * 60 * 1_000_000_000; // Approximate
                current_number.clear();
            }
            'M' => {
                let value: i64 = current_number.parse().unwrap_or_else(|_| panic!("Invalid value"));
                if in_time_part {
                    total_nanos += value * 60 * 1_000_000_000; // Minutes
                } else {
                    total_nanos += value * 30 * 24 * 60 * 60 * 1_000_000_000; // Months (approximate)
                }
                current_number.clear();
            }
            'W' => {
                if in_time_part {
                    panic!("Weeks not allowed in time part");
                }
                let weeks: i64 =
                    current_number.parse().unwrap_or_else(|_| panic!("Invalid week value"));
                total_nanos += weeks * 7 * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'D' => {
                if in_time_part {
                    panic!("Days not allowed in time part");
                }
                let days: i64 =
                    current_number.parse().unwrap_or_else(|_| panic!("Invalid day value"));
                total_nanos += days * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'H' => {
                if !in_time_part {
                    panic!("Hours only allowed in time part");
                }
                let hours: i64 =
                    current_number.parse().unwrap_or_else(|_| panic!("Invalid hour value"));
                total_nanos += hours * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'S' => {
                if !in_time_part {
                    panic!("Seconds only allowed in time part");
                }
                let seconds: i64 =
                    current_number.parse().unwrap_or_else(|_| panic!("Invalid second value"));
                total_nanos += seconds * 1_000_000_000;
                current_number.clear();
            }
            _ => {
                panic!("Invalid character in interval");
            }
        }
    }

    if !current_number.is_empty() {
        panic!("Incomplete interval specification");
    }

    Ok(Interval::from_nanos(total_nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::{SpanColumn, SpanLine};

    fn make_span(value: &str) -> Span {
        Span { column: SpanColumn(0), line: SpanLine(1), fragment: value.to_string() }
    }

    #[test]
    fn test_days() {
        let span = make_span("P1D");
        let interval = parse_interval(&span).unwrap();
        // 1 day = 24 * 60 * 60 * 1_000_000_000 nanos
        assert_eq!(interval.to_nanos(), 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_time_hours_minutes() {
        let span = make_span("PT2H30M");
        let interval = parse_interval(&span).unwrap();
        // 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
        assert_eq!(interval.to_nanos(), (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
    }

    #[test]
    fn test_complex() {
        let span = make_span("P1DT2H30M");
        let interval = parse_interval(&span).unwrap();
        // 1 day + 2 hours + 30 minutes
        let expected = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000;
        assert_eq!(interval.to_nanos(), expected);
    }

    #[test]
    fn test_seconds_only() {
        let span = make_span("PT45S");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 45 * 1_000_000_000);
    }

    #[test]
    fn test_minutes_only() {
        let span = make_span("PT5M");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 5 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_hours_only() {
        let span = make_span("PT1H");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_weeks() {
        let span = make_span("P1W");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 7 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_years() {
        let span = make_span("P1Y");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 365 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_months() {
        let span = make_span("P1M");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 30 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_full_format() {
        let span = make_span("P1Y2M3DT4H5M6S");
        let interval = parse_interval(&span).unwrap();
        let expected = 365 * 24 * 60 * 60 * 1_000_000_000 +     // 1 year
                      2 * 30 * 24 * 60 * 60 * 1_000_000_000 +  // 2 months
                      3 * 24 * 60 * 60 * 1_000_000_000 +       // 3 days
                      4 * 60 * 60 * 1_000_000_000 +            // 4 hours
                      5 * 60 * 1_000_000_000 +                 // 5 minutes
                      6 * 1_000_000_000; // 6 seconds
        assert_eq!(interval.to_nanos(), expected);
    }

    #[test]
    #[should_panic(expected = "Invalid interval format - must start with P")]
    fn test_invalid_format() {
        let span = make_span("invalid");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid character in interval")]
    fn test_invalid_character() {
        let span = make_span("P1X");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Years not allowed in time part")]
    fn test_years_in_time_part() {
        let span = make_span("P1TY");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Weeks not allowed in time part")]
    fn test_weeks_in_time_part() {
        let span = make_span("P1TW");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Days not allowed in time part")]
    fn test_days_in_time_part() {
        let span = make_span("P1TD");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Hours only allowed in time part")]
    fn test_hours_in_date_part() {
        let span = make_span("P1H");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Seconds only allowed in time part")]
    fn test_seconds_in_date_part() {
        let span = make_span("P1S");
        parse_interval(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Incomplete interval specification")]
    fn test_incomplete_specification() {
        let span = make_span("P1");
        parse_interval(&span).unwrap();
    }
}
