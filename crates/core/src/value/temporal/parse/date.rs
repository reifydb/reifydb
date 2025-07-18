// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::temporal;
use crate::{Date, Error, Span};

pub fn parse_date(span: &Span) -> Result<Date, Error> {
    let span_parts = span.split('-');
    if span_parts.len() != 3 {
        return Err(Error(temporal::invalid_date_format(span.clone())));
    }

    // Check for empty parts
    if span_parts[0].fragment.trim().is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[0].clone())));
    }
    if span_parts[1].fragment.trim().is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[1].clone())));
    }
    if span_parts[2].fragment.trim().is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[2].clone())));
    }

    let year_str = span_parts[0].fragment.trim();
    if year_str.len() != 4 {
        return Err(Error(temporal::invalid_year(span_parts[0].clone())));
    }

    let year = year_str
        .parse::<i32>()
        .map_err(|_| Error(temporal::invalid_year(span_parts[0].clone())))?;

    let month_str = span_parts[1].fragment.trim();
    if month_str.len() != 2 {
        return Err(Error(temporal::invalid_month(span_parts[1].clone())));
    }

    let month = month_str
        .parse::<u32>()
        .map_err(|_| Error(temporal::invalid_month(span_parts[1].clone())))?;

    let day_str = span_parts[2].fragment.trim();
    if day_str.len() != 2 {
        return Err(Error(temporal::invalid_day(span_parts[2].clone())));
    }

    let day =
        day_str.parse::<u32>().map_err(|_| Error(temporal::invalid_day(span_parts[2].clone())))?;

    Date::new(year, month, day).ok_or_else(|| Error(temporal::invalid_date_values(span.clone())))
}

#[cfg(test)]
mod tests {
    use super::parse_date;
    use crate::Span;

    #[test]
    fn test_basic() {
        let span = Span::testing("2024-03-15");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-03-15");
    }

    #[test]
    fn test_leap_year() {
        let span = Span::testing("2024-02-29");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-02-29");
    }

    #[test]
    fn test_boundaries() {
        let span = Span::testing("2000-01-01");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2000-01-01");

        let span = Span::testing("2024-12-31");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-12-31");
    }

    #[test]
    fn test_invalid_format() {
        let span = Span::testing("2024-03");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_001");
    }

    #[test]
    fn test_invalid_year() {
        let span = Span::testing("abcd-03-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_005");
    }

    #[test]
    fn test_invalid_month() {
        let span = Span::testing("2024-invalid-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_006");
    }

    #[test]
    fn test_invalid_day() {
        let span = Span::testing("2024-03-invalid");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_007");
    }

    #[test]
    fn test_invalid_date_values() {
        let span = Span::testing("2024-13-32");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_012");
    }

    #[test]
    fn test_four_digit_year() {
        // Test 2-digit year
        let span = Span::testing("24-03-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_005");

        // Test 3-digit year
        let span = Span::testing("024-03-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_005");

        // Test 5-digit year
        let span = Span::testing("20240-03-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_005");

        // Test year with leading zeros (still 4 digits, should work)
        let span = Span::testing("0024-03-15");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "0024-03-15");
    }

    #[test]
    fn test_two_digit_month() {
        // Test 1-digit month
        let span = Span::testing("2024-3-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_006");

        // Test 3-digit month
        let span = Span::testing("2024-003-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_006");

        // Test month with leading zeros (still 2 digits, should work)
        let span = Span::testing("2024-03-15");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-03-15");

        // Test month with non-digits
        let span = Span::testing("2024-0a-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_006");

        // Test month with spaces
        let span = Span::testing("2024- 3-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_006");
    }

    #[test]
    fn test_two_digit_day() {
        // Test 1-digit day
        let span = Span::testing("2024-03-5");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_007");

        // Test 3-digit day
        let span = Span::testing("2024-03-015");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_007");

        // Test day with leading zeros (still 2 digits, should work)
        let span = Span::testing("2024-03-05");
        let date = parse_date(&span).unwrap();
        assert_eq!(date.to_string(), "2024-03-05");

        // Test day with non-digits
        let span = Span::testing("2024-03-1a");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_007");

        // Test day with spaces
        let span = Span::testing("2024-03- 5");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_007");
    }
}
