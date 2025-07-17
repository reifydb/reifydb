// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Error, Interval, Span};
use crate::diagnostic::temporal;

pub fn parse_interval(span: &Span) -> Result<Interval, Error> {
    let fragment = &span.fragment;
    // Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)

    if fragment.len() == 1 || !fragment.starts_with('P')  || fragment == "PT"{
        return Err(Error(temporal::invalid_interval_format(span.clone())));
    }

    let mut chars = fragment.chars().skip(1); // Skip 'P'
    let mut total_nanos = 0i64;
    let mut current_number = String::new();
    let mut in_time_part = false;
    let mut current_position = 1; // Start after 'P'

    while let Some(c) = chars.next() {
        match c {
            'T' => {
                in_time_part = true;
                current_position += 1;
            }
            '0'..='9' => {
                current_number.push(c);
                current_position += 1;
            }
            'Y' => {
                if in_time_part {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::invalid_unit_in_context(unit_span, 'Y', true)));
                }
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let years: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'Y'))
                })?;
                total_nanos += years * 365 * 24 * 60 * 60 * 1_000_000_000; // Approximate
                current_number.clear();
                current_position += 1;
            }
            'M' => {
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let value: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'M'))
                })?;
                if in_time_part {
                    total_nanos += value * 60 * 1_000_000_000; // Minutes
                } else {
                    total_nanos += value * 30 * 24 * 60 * 60 * 1_000_000_000; // Months (approximate)
                }
                current_number.clear();
                current_position += 1;
            }
            'W' => {
                if in_time_part {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::invalid_unit_in_context(unit_span, 'W', true)));
                }
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let weeks: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'W'))
                })?;
                total_nanos += weeks * 7 * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
                current_position += 1;
            }
            'D' => {
                if in_time_part {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::invalid_unit_in_context(unit_span, 'D', true)));
                }
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let days: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'D'))
                })?;
                total_nanos += days * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
                current_position += 1;
            }
            'H' => {
                if !in_time_part {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::invalid_unit_in_context(unit_span, 'H', false)));
                }
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let hours: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'H'))
                })?;
                total_nanos += hours * 60 * 60 * 1_000_000_000;
                current_number.clear();
                current_position += 1;
            }
            'S' => {
                if !in_time_part {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::invalid_unit_in_context(unit_span, 'S', false)));
                }
                if current_number.is_empty() {
                    let unit_span = span.sub_span(current_position, 1);
                    return Err(Error(temporal::incomplete_interval_specification(unit_span)));
                }
                let seconds: i64 = current_number.parse().map_err(|_| {
                    let number_span = span
                        .sub_span(current_position - current_number.len(), current_number.len());
                    Error(temporal::invalid_interval_component_value(number_span, 'S'))
                })?;
                total_nanos += seconds * 1_000_000_000;
                current_number.clear();
                current_position += 1;
            }
            _ => {
                let char_span = span.sub_span(current_position, 1);
                return Err(Error(temporal::invalid_interval_character(char_span)));
            }
        }
    }

    if !current_number.is_empty() {
        let number_span =
            span.sub_span(current_position - current_number.len(), current_number.len());
        return Err(Error(temporal::incomplete_interval_specification(number_span)));
    }

    Ok(Interval::from_nanos(total_nanos))
}

#[cfg(test)]
mod tests {
    use super::parse_interval;
    use crate::Span;

    #[test]
    fn test_days() {
        let span = Span::testing("P1D");
        let interval = parse_interval(&span).unwrap();
        // 1 day = 24 * 60 * 60 * 1_000_000_000 nanos
        assert_eq!(interval.to_nanos(), 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_time_hours_minutes() {
        let span = Span::testing("PT2H30M");
        let interval = parse_interval(&span).unwrap();
        // 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
        assert_eq!(interval.to_nanos(), (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
    }

    #[test]
    fn test_complex() {
        let span = Span::testing("P1DT2H30M");
        let interval = parse_interval(&span).unwrap();
        // 1 day + 2 hours + 30 minutes
        let expected = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000;
        assert_eq!(interval.to_nanos(), expected);
    }

    #[test]
    fn test_seconds_only() {
        let span = Span::testing("PT45S");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 45 * 1_000_000_000);
    }

    #[test]
    fn test_minutes_only() {
        let span = Span::testing("PT5M");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 5 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_hours_only() {
        let span = Span::testing("PT1H");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_weeks() {
        let span = Span::testing("P1W");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 7 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_years() {
        let span = Span::testing("P1Y");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 365 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_months() {
        let span = Span::testing("P1M");
        let interval = parse_interval(&span).unwrap();
        assert_eq!(interval.to_nanos(), 30 * 24 * 60 * 60 * 1_000_000_000);
    }

    #[test]
    fn test_full_format() {
        let span = Span::testing("P1Y2M3DT4H5M6S");
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
    fn test_invalid_format() {
        let span = Span::testing("invalid");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_004");
    }

    #[test]
    fn test_invalid_character() {
        let span = Span::testing("P1X");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_014");
    }

    #[test]
    fn test_years_in_time_part() {
        let span = Span::testing("PTY");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_016");
    }

    #[test]
    fn test_weeks_in_time_part() {
        let span = Span::testing("PTW");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_016");
    }

    #[test]
    fn test_days_in_time_part() {
        let span = Span::testing("PTD");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_016");
    }

    #[test]
    fn test_hours_in_date_part() {
        let span = Span::testing("P1H");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_016");
    }

    #[test]
    fn test_seconds_in_date_part() {
        let span = Span::testing("P1S");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_016");
    }

    #[test]
    fn test_incomplete_specification() {
        let span = Span::testing("P1");
        let err = parse_interval(&span).unwrap_err();
        assert_eq!(err.0.code, "TEMPORAL_015");
    }
}