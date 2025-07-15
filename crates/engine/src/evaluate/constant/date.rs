// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use reifydb_core::{Date, Span};

pub(crate) fn parse_date(span: &Span) -> evaluate::Result<Date> {
    let fragment = &span.fragment;
    // Parse date in format YYYY-MM-DD
    let parts: Vec<&str> = fragment.split('-').collect();
    if parts.len() != 3 {
        panic!("Invalid date format");
    }

    let year = parts[0].parse::<i32>().unwrap_or_else(|_| panic!("Invalid year"));
    let month = parts[1].parse::<u32>().unwrap_or_else(|_| panic!("Invalid month"));
    let day = parts[2].parse::<u32>().unwrap_or_else(|_| panic!("Invalid day"));

    Ok(Date::new(year, month, day).unwrap_or_else(|| panic!("Invalid date")))
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
        let span = make_span("2024-03-15");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-03-15");
    }

    #[test]
    fn test_leap_year() {
        let span = make_span("2024-02-29");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-02-29");
    }

    #[test]
    fn test_boundaries() {
        let span = make_span("2000-01-01");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2000-01-01");

        let span = make_span("2024-12-31");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-12-31");
    }

    #[test]
    #[should_panic(expected = "Invalid date format")]
    fn test_invalid_format() {
        let span = make_span("2024-03");
        parse_date(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid year")]
    fn test_invalid_year() {
        let span = make_span("invalid-03-15");
        parse_date(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid month")]
    fn test_invalid_month() {
        let span = make_span("2024-invalid-15");
        parse_date(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid day")]
    fn test_invalid_day() {
        let span = make_span("2024-03-invalid");
        parse_date(&span).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid date")]
    fn test_invalid_date_values() {
        let span = make_span("2024-13-32");
        parse_date(&span).unwrap();
    }
}
