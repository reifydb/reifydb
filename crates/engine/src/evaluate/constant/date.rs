// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::Error;
use reifydb_core::{Date, Span};
use reifydb_core::diagnostic::temporal;

pub(crate) fn parse_date(span: &Span) -> evaluate::Result<Date> {
    let span_parts = span.split('-');
    if span_parts.len() != 3 {
        return Err(Error(temporal::invalid_date_format(span.clone())));
    }

    // Check for empty parts
    if span_parts[0].fragment.is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[0].clone())));
    }
    if span_parts[1].fragment.is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[1].clone())));
    }
    if span_parts[2].fragment.is_empty() {
        return Err(Error(temporal::empty_date_component(span_parts[2].clone())));
    }

    let year = span_parts[0]
        .fragment
        .parse::<i32>()
        .map_err(|_| Error(temporal::invalid_year(span_parts[0].clone())))?;

    let month = span_parts[1]
        .fragment
        .parse::<u32>()
        .map_err(|_| Error(temporal::invalid_month(span_parts[1].clone())))?;

    let day = span_parts[2]
        .fragment
        .parse::<u32>()
        .map_err(|_| Error(temporal::invalid_day(span_parts[2].clone())))?;

    Date::new(year, month, day).ok_or_else(|| Error(temporal::invalid_date_values(span.clone())))
}

#[cfg(test)]
mod tests {
    use crate::evaluate::constant::date::parse_date;
    use reifydb_core::Span;

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
        assert_eq!(err.code, "TEMPORAL_001");
    }

    #[test]
    fn test_invalid_year() {
        let span = Span::testing("invalid-03-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_005");
    }

    #[test]
    fn test_invalid_month() {
        let span = Span::testing("2024-invalid-15");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_006");
    }

    #[test]
    fn test_invalid_day() {
        let span = Span::testing("2024-03-invalid");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_007");
    }

    #[test]
    fn test_invalid_date_values() {
        let span = Span::testing("2024-13-32");
        let err = parse_date(&span).unwrap_err();
        assert_eq!(err.code, "TEMPORAL_012");
    }
}
