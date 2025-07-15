// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{Error, EvaluationContext, Evaluator};
use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::num::parse_float;
use reifydb_core::{DataType, Span};
use reifydb_core::{Date, DateTime, Interval, Time};
use reifydb_diagnostic::r#type::{OutOfRange, out_of_range};
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: &ConstantExpression,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value(&expr, row_count)?,
        })
    }

    pub(crate) fn constant_of(
        &mut self,
        expr: &ConstantExpression,
        data_type: DataType,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value_of(&expr, data_type, row_count)?,
        })
    }

    // FIXME rather than static parsing - it should use the context it is in, this will avoid data wrangling down the line
    fn constant_value(
        expr: &ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }
            ConstantExpression::Number { span } => {
                let s = &span.fragment.replace("_", "");

                if s.contains(".") || s.contains("e") {
                    if let Ok(v) = parse_float(s) {
                        return Ok(ColumnValues::float8(vec![v; row_count]));
                    }
                    return Err(Error(out_of_range(OutOfRange {
                        span: expr.span(),
                        column: None,
                        data_type: Some(DataType::Float8),
                    })));
                }

                if let Ok(v) = s.parse::<i8>() {
                    ColumnValues::int1(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i16>() {
                    ColumnValues::int2(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i32>() {
                    ColumnValues::int4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i64>() {
                    ColumnValues::int8(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i128>() {
                    ColumnValues::int16(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u128>() {
                    ColumnValues::uint16(vec![v; row_count])
                } else {
                    return Err(Error(out_of_range(OutOfRange {
                        span: expr.span(),
                        column: None,
                        data_type: Some(DataType::Uint16),
                    })));
                }
            }
            ConstantExpression::Text { span } => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Temporal { span } => {
                Self::parse_temporal_constant(&span.fragment, row_count)?
            }
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }

    fn constant_value_of(
        expr: &ConstantExpression,
        data_type: DataType,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match (expr, data_type) {
            (ConstantExpression::Bool { span }, DataType::Bool) => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }

            (ConstantExpression::Number { span }, ty) => {
                let s = &span.fragment.replace("_", "");
                match ty {
                    DataType::Float4 => match s.parse::<f32>() {
                        Ok(v) => ColumnValues::float4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Float8 => match s.parse::<f64>() {
                        Ok(v) => ColumnValues::float8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Int1 => match s.parse::<i8>() {
                        Ok(v) => ColumnValues::int1(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Int2 => match s.parse::<i16>() {
                        Ok(v) => ColumnValues::int2(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Int4 => match s.parse::<i32>() {
                        Ok(v) => ColumnValues::int4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Int8 => match s.parse::<i64>() {
                        Ok(v) => ColumnValues::int8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Int16 => match s.parse::<i128>() {
                        Ok(v) => ColumnValues::int16(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Uint1 => match s.parse::<u8>() {
                        Ok(v) => ColumnValues::uint1(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Uint2 => match s.parse::<u16>() {
                        Ok(v) => ColumnValues::uint2(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Uint4 => match s.parse::<u32>() {
                        Ok(v) => ColumnValues::uint4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Uint8 => match s.parse::<u64>() {
                        Ok(v) => ColumnValues::uint8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },
                    DataType::Uint16 => match s.parse::<u128>() {
                        Ok(v) => ColumnValues::uint16(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(out_of_range(OutOfRange {
                                span: span.clone(),
                                column: None,
                                data_type: Some(ty),
                            })));
                        }
                    },

                    _ => {
                        return Err(Error(out_of_range(OutOfRange {
                            span: span.clone(),
                            column: None,
                            data_type: Some(ty),
                        })));
                    }
                }
            }

            (ConstantExpression::Text { span }, DataType::Utf8) => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }

            (ConstantExpression::Temporal { span }, DataType::Date) => {
                Self::parse_temporal_constant_as_date(span, row_count)?
            }
            (ConstantExpression::Temporal { span }, DataType::DateTime) => {
                Self::parse_temporal_constant_as_datetime(span, row_count)?
            }
            (ConstantExpression::Temporal { span }, DataType::Time) => {
                Self::parse_temporal_constant_as_time(span, row_count)?
            }
            (ConstantExpression::Temporal { span }, DataType::Interval) => {
                Self::parse_temporal_constant_as_interval(span, row_count)?
            }

            (ConstantExpression::Undefined { .. }, _) => ColumnValues::Undefined(row_count),

            (_, data_type) => {
                return Err(Error(out_of_range(OutOfRange {
                    span: expr.span(),
                    column: None,
                    data_type: Some(data_type),
                })));
            }
        })
    }

    fn parse_temporal_constant(fragment: &str, row_count: usize) -> evaluate::Result<ColumnValues> {
        // Try to parse as different temporal types in order of specificity
        if let Ok(datetime) = Self::parse_datetime_from_fragment(fragment) {
            return Ok(ColumnValues::datetime(vec![datetime; row_count]));
        }

        if let Ok(date) = Self::parse_date_from_fragment(fragment) {
            return Ok(ColumnValues::date(vec![date; row_count]));
        }

        if let Ok(time) = Self::parse_time_from_fragment(fragment) {
            return Ok(ColumnValues::time(vec![time; row_count]));
        }

        if let Ok(interval) = Self::parse_interval_from_fragment(fragment) {
            return Ok(ColumnValues::interval(vec![interval; row_count]));
        }

        // If none of the parsing attempts succeeded, return an error
        Err(Error(out_of_range(OutOfRange {
            span: Span {
                line: reifydb_core::SpanLine(1),
                column: reifydb_core::SpanColumn(0),
                fragment: fragment.to_string(),
            },
            column: None,
            data_type: None,
        })))
    }

    fn parse_temporal_constant_as_date(
        span: &Span,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        match Self::parse_date_from_fragment(&span.fragment) {
            Ok(date) => Ok(ColumnValues::date(vec![date; row_count])),
            Err(_) => Err(Error(out_of_range(OutOfRange {
                span: span.clone(),
                column: None,
                data_type: Some(DataType::Date),
            }))),
        }
    }

    fn parse_temporal_constant_as_datetime(
        span: &Span,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        match Self::parse_datetime_from_fragment(&span.fragment) {
            Ok(datetime) => Ok(ColumnValues::datetime(vec![datetime; row_count])),
            Err(_) => Err(Error(out_of_range(OutOfRange {
                span: span.clone(),
                column: None,
                data_type: Some(DataType::DateTime),
            }))),
        }
    }

    fn parse_temporal_constant_as_time(
        span: &Span,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        match Self::parse_time_from_fragment(&span.fragment) {
            Ok(time) => Ok(ColumnValues::time(vec![time; row_count])),
            Err(_) => Err(Error(out_of_range(OutOfRange {
                span: span.clone(),
                column: None,
                data_type: Some(DataType::Time),
            }))),
        }
    }

    fn parse_temporal_constant_as_interval(
        span: &Span,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        match Self::parse_interval_from_fragment(&span.fragment) {
            Ok(interval) => Ok(ColumnValues::interval(vec![interval; row_count])),
            Err(_) => Err(Error(out_of_range(OutOfRange {
                span: span.clone(),
                column: None,
                data_type: Some(DataType::Interval),
            }))),
        }
    }

    fn parse_date_from_fragment(fragment: &str) -> Result<Date, &'static str> {
        // Parse date in format YYYY-MM-DD
        let parts: Vec<&str> = fragment.split('-').collect();
        if parts.len() != 3 {
            return Err("Invalid date format");
        }

        let year = parts[0].parse::<i32>().map_err(|_| "Invalid year")?;
        let month = parts[1].parse::<u32>().map_err(|_| "Invalid month")?;
        let day = parts[2].parse::<u32>().map_err(|_| "Invalid day")?;

        Date::new(year, month, day).ok_or("Invalid date")
    }

    fn parse_datetime_from_fragment(fragment: &str) -> Result<DateTime, &'static str> {
        // Parse datetime in format YYYY-MM-DDTHH:MM:SS
        let parts: Vec<&str> = fragment.split('T').collect();
        if parts.len() != 2 {
            return Err("Invalid datetime format");
        }

        let date_part = parts[0];
        let time_part = parts[1];

        // Parse date part
        let date_parts: Vec<&str> = date_part.split('-').collect();
        if date_parts.len() != 3 {
            return Err("Invalid date format in datetime");
        }

        let year = date_parts[0].parse::<i32>().map_err(|_| "Invalid year")?;
        let month = date_parts[1].parse::<u32>().map_err(|_| "Invalid month")?;
        let day = date_parts[2].parse::<u32>().map_err(|_| "Invalid day")?;

        // Parse time part
        let time_parts: Vec<&str> = time_part.split(':').collect();
        if time_parts.len() != 3 {
            return Err("Invalid time format in datetime");
        }

        let hour = time_parts[0].parse::<u32>().map_err(|_| "Invalid hour")?;
        let minute = time_parts[1].parse::<u32>().map_err(|_| "Invalid minute")?;
        let second = time_parts[2].parse::<u32>().map_err(|_| "Invalid second")?;

        DateTime::new(year, month, day, hour, minute, second, 0).ok_or("Invalid datetime")
    }

    fn parse_time_from_fragment(fragment: &str) -> Result<Time, &'static str> {
        // Parse time in format HH:MM:SS
        let parts: Vec<&str> = fragment.split(':').collect();
        if parts.len() != 3 {
            return Err("Invalid time format");
        }

        let hour = parts[0].parse::<u32>().map_err(|_| "Invalid hour")?;
        let minute = parts[1].parse::<u32>().map_err(|_| "Invalid minute")?;
        let second = parts[2].parse::<u32>().map_err(|_| "Invalid second")?;

        Time::new(hour, minute, second, 0).ok_or("Invalid time")
    }

    fn parse_interval_from_fragment(fragment: &str) -> Result<Interval, &'static str> {
        // Parse ISO 8601 duration format (P1D, PT2H30M, P1Y2M3DT4H5M6S)
        if !fragment.starts_with('P') {
            return Err("Invalid interval format - must start with P");
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
                        return Err("Years not allowed in time part");
                    }
                    let years: i64 = current_number.parse().map_err(|_| "Invalid year value")?;
                    total_nanos += years * 365 * 24 * 60 * 60 * 1_000_000_000; // Approximate
                    current_number.clear();
                }
                'M' => {
                    let value: i64 = current_number.parse().map_err(|_| "Invalid value")?;
                    if in_time_part {
                        total_nanos += value * 60 * 1_000_000_000; // Minutes
                    } else {
                        total_nanos += value * 30 * 24 * 60 * 60 * 1_000_000_000; // Months (approximate)
                    }
                    current_number.clear();
                }
                'W' => {
                    if in_time_part {
                        return Err("Weeks not allowed in time part");
                    }
                    let weeks: i64 = current_number.parse().map_err(|_| "Invalid week value")?;
                    total_nanos += weeks * 7 * 24 * 60 * 60 * 1_000_000_000;
                    current_number.clear();
                }
                'D' => {
                    if in_time_part {
                        return Err("Days not allowed in time part");
                    }
                    let days: i64 = current_number.parse().map_err(|_| "Invalid day value")?;
                    total_nanos += days * 24 * 60 * 60 * 1_000_000_000;
                    current_number.clear();
                }
                'H' => {
                    if !in_time_part {
                        return Err("Hours only allowed in time part");
                    }
                    let hours: i64 = current_number.parse().map_err(|_| "Invalid hour value")?;
                    total_nanos += hours * 60 * 60 * 1_000_000_000;
                    current_number.clear();
                }
                'S' => {
                    if !in_time_part {
                        return Err("Seconds only allowed in time part");
                    }
                    let seconds: i64 =
                        current_number.parse().map_err(|_| "Invalid second value")?;
                    total_nanos += seconds * 1_000_000_000;
                    current_number.clear();
                }
                _ => {
                    return Err("Invalid character in interval");
                }
            }
        }

        if !current_number.is_empty() {
            return Err("Incomplete interval specification");
        }

        Ok(Interval::from_nanos(total_nanos))
    }
}

#[cfg(test)]
mod tests {
    use reifydb_core::{Span, SpanColumn, SpanLine};

    mod constant_value {
        use crate::evaluate::Evaluator;
        use crate::evaluate::constant::ConstantExpression;
        use crate::evaluate::constant::tests::make_span;
        use crate::frame::ColumnValues;

        #[test]
        fn test_bool_true() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            let col = Evaluator::constant_value(&expr, 3).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![true; 3]));
        }

        #[test]
        fn test_bool_false() {
            let expr = ConstantExpression::Bool { span: make_span("false") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![false; 2]));
        }

        #[test]
        fn test_float8() {
            let expr = ConstantExpression::Number { span: make_span("3.14") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::float8(vec![3.14; 2]));
        }

        #[test]
        fn test_int1() {
            let expr = ConstantExpression::Number { span: make_span("127") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int1(vec![127]));
        }

        #[test]
        fn test_int2() {
            let expr = ConstantExpression::Number { span: make_span("32767") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::int2(vec![32767; 2]));
        }

        #[test]
        fn test_int4() {
            let expr = ConstantExpression::Number { span: make_span("2147483647") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int4(vec![2147483647]));
        }

        #[test]
        fn test_int8() {
            let expr = ConstantExpression::Number { span: make_span("9223372036854775807") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int8(vec![9223372036854775807]));
        }

        #[test]
        fn test_int16() {
            let expr = ConstantExpression::Number {
                span: make_span("170141183460469231731687303715884105727"),
            };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int16(vec![170141183460469231731687303715884105727i128]));
        }

        #[test]
        fn test_uint16() {
            let expr = ConstantExpression::Number { span: make_span(&u128::MAX.to_string()) };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::uint16(vec![340282366920938463463374607431768211455]));
        }

        #[test]
        fn test_invalid_number_fallback_to_undefined() {
            let expr = ConstantExpression::Number { span: make_span("not_a_number") };
            let err = Evaluator::constant_value(&expr, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_string() {
            let expr = ConstantExpression::Text { span: make_span("hello") };
            let col = Evaluator::constant_value(&expr, 3).unwrap();
            assert_eq!(
                col,
                ColumnValues::utf8(["hello".to_string(), "hello".to_string(), "hello".to_string()])
            );
        }

        #[test]
        fn test_undefined() {
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::Undefined(2));
        }

        #[test]
        fn test_temporal_date() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            match col {
                ColumnValues::Date(values, validity) => {
                    assert_eq!(values.len(), 2);
                    assert_eq!(validity.len(), 2);
                    assert_eq!(validity[0], true);
                    assert_eq!(validity[1], true);
                    assert_eq!(values[0].to_string(), "2024-03-15");
                    assert_eq!(values[1].to_string(), "2024-03-15");
                }
                _ => panic!("Expected Date column"),
            }
        }

        #[test]
        fn test_temporal_datetime() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15T14:30:00") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            match col {
                ColumnValues::DateTime(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    assert_eq!(values[0].to_string(), "2024-03-15T14:30:00.000000000Z");
                }
                _ => panic!("Expected DateTime column"),
            }
        }

        #[test]
        fn test_temporal_time() {
            let expr = ConstantExpression::Temporal { span: make_span("14:30:00") };
            let col = Evaluator::constant_value(&expr, 3).unwrap();
            match col {
                ColumnValues::Time(values, validity) => {
                    assert_eq!(values.len(), 3);
                    assert_eq!(validity.len(), 3);
                    assert_eq!(validity[0], true);
                    assert_eq!(validity[1], true);
                    assert_eq!(validity[2], true);
                    assert_eq!(values[0].to_string(), "14:30:00.000000000");
                    assert_eq!(values[1].to_string(), "14:30:00.000000000");
                    assert_eq!(values[2].to_string(), "14:30:00.000000000");
                }
                _ => panic!("Expected Time column"),
            }
        }

        #[test]
        fn test_temporal_interval_days() {
            let expr = ConstantExpression::Temporal { span: make_span("P1D") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            match col {
                ColumnValues::Interval(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    // 1 day = 24 * 60 * 60 * 1_000_000_000 nanos
                    assert_eq!(values[0].to_nanos(), 24 * 60 * 60 * 1_000_000_000);
                }
                _ => panic!("Expected Interval column"),
            }
        }

        #[test]
        fn test_temporal_interval_time() {
            let expr = ConstantExpression::Temporal { span: make_span("PT2H30M") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            match col {
                ColumnValues::Interval(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    // 2 hours 30 minutes = (2 * 60 * 60 + 30 * 60) * 1_000_000_000 nanos
                    assert_eq!(values[0].to_nanos(), (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
                }
                _ => panic!("Expected Interval column"),
            }
        }

        #[test]
        fn test_temporal_interval_complex() {
            let expr = ConstantExpression::Temporal { span: make_span("P1DT2H30M") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            match col {
                ColumnValues::Interval(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    // 1 day + 2 hours + 30 minutes
                    let expected = (24 * 60 * 60 + 2 * 60 * 60 + 30 * 60) * 1_000_000_000;
                    assert_eq!(values[0].to_nanos(), expected);
                }
                _ => panic!("Expected Interval column"),
            }
        }

        #[test]
        fn test_temporal_invalid_format() {
            let expr = ConstantExpression::Temporal { span: make_span("invalid-format") };
            let err = Evaluator::constant_value(&expr, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_temporal_invalid_date() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-13-32") };
            let err = Evaluator::constant_value(&expr, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_temporal_invalid_time() {
            let expr = ConstantExpression::Temporal { span: make_span("25:70:80") };
            let err = Evaluator::constant_value(&expr, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }
    }

    mod constant_value_of {
        use crate::evaluate::Evaluator;
        use crate::evaluate::constant::tests::make_span;
        use crate::frame::ColumnValues;
        use reifydb_core::DataType;
        use reifydb_rql::expression::ConstantExpression;

        #[test]
        fn test_bool_true() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            let col = Evaluator::constant_value_of(&expr, DataType::Bool, 3).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![true; 3]));
        }

        #[test]
        fn test_bool_mismatch() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            assert!(Evaluator::constant_value_of(&expr, DataType::Int1, 1).is_err());
        }

        #[test]
        fn test_int1_ok() {
            number_ok("127", DataType::Int1, 2, ColumnValues::int1(vec![127; 2]));
        }
        #[test]
        fn test_int1_type_mismatch() {
            number_type_mismatch("128", DataType::Int1);
        }

        #[test]
        fn test_int2_ok() {
            number_ok("32767", DataType::Int2, 1, ColumnValues::int2(vec![32767]));
        }
        #[test]
        fn test_int2_type_mismatch() {
            number_type_mismatch("40000", DataType::Int2);
        }

        #[test]
        fn test_int4_ok() {
            number_ok("2147483647", DataType::Int4, 1, ColumnValues::int4(vec![2147483647]));
        }
        #[test]
        fn test_int4_type_mismatch() {
            number_type_mismatch("9999999999", DataType::Int4);
        }

        #[test]
        fn test_int8_ok() {
            number_ok(
                "9223372036854775807",
                DataType::Int8,
                1,
                ColumnValues::int8(vec![9223372036854775807]),
            );
        }
        #[test]
        fn test_int8_type_mismatch() {
            number_type_mismatch("999999999999999999999", DataType::Int8);
        }

        #[test]
        fn test_int16_ok() {
            number_ok(
                "170141183460469231731687303715884105727",
                DataType::Int16,
                1,
                ColumnValues::int16(vec![i128::MAX]),
            );
        }
        #[test]
        fn test_int16_type_mismatch() {
            number_type_mismatch("a", DataType::Int16);
        }

        #[test]
        fn test_uint1_ok() {
            number_ok("255", DataType::Uint1, 2, ColumnValues::uint1(vec![255; 2]));
        }

        #[test]
        fn test_uint1_type_mismatch() {
            number_type_mismatch("-1", DataType::Uint1);
        }

        #[test]
        fn test_uint2_ok() {
            number_ok("65535", DataType::Uint2, 1, ColumnValues::uint2(vec![65535]));
        }
        #[test]
        fn test_uint2_type_mismatch() {
            number_type_mismatch("70000", DataType::Uint2);
        }

        #[test]
        fn test_uint4_ok() {
            number_ok("4294967295", DataType::Uint4, 1, ColumnValues::uint4(vec![4294967295]));
        }
        #[test]
        fn test_uint4_type_mismatch() {
            number_type_mismatch("9999999999", DataType::Uint4);
        }

        #[test]
        fn test_uint8_ok() {
            number_ok(
                "18446744073709551615",
                DataType::Uint8,
                1,
                ColumnValues::uint8(vec![u64::MAX]),
            );
        }
        #[test]
        fn test_uint8_type_mismatch() {
            number_type_mismatch("-1", DataType::Uint8);
        }

        #[test]
        fn test_uint16_ok() {
            number_ok(
                "340282366920938463463374607431768211455",
                DataType::Uint16,
                1,
                ColumnValues::uint16(vec![u128::MAX]),
            );
        }
        #[test]
        fn test_uint16_type_mismatch() {
            number_type_mismatch("z", DataType::Uint16);
        }

        #[test]
        fn test_float4_ok() {
            number_ok("3.14", DataType::Float4, 2, ColumnValues::float4(vec![3.14; 2]));
        }
        #[test]
        fn test_float4_type_mismatch() {
            number_type_mismatch("not_a_float", DataType::Float4);
        }

        #[test]
        fn test_float8_ok() {
            number_ok("3.14", DataType::Float8, 2, ColumnValues::float8(vec![3.14; 2]));
        }
        #[test]
        fn test_float8_type_mismatch() {
            number_type_mismatch("not_a_float", DataType::Float8);
        }

        #[test]
        fn test_text_ok() {
            let expr = ConstantExpression::Text { span: make_span("hello") };
            let col = Evaluator::constant_value_of(&expr, DataType::Utf8, 3).unwrap();
            assert_eq!(col, ColumnValues::utf8(vec!["hello".to_string(); 3]));
        }

        #[test]
        fn test_text_mismatch() {
            let expr = ConstantExpression::Text { span: make_span("text") };
            assert!(Evaluator::constant_value_of(&expr, DataType::Int1, 1).is_err());
        }

        #[test]
        fn test_undefined_ok() {
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value_of(&expr, DataType::Undefined, 5).unwrap();
            assert_eq!(col, ColumnValues::Undefined(5));
        }

        #[test]
        fn test_undefined_different_kind() {
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value_of(&expr, DataType::Float8, 5).unwrap();
            assert_eq!(col, ColumnValues::Undefined(5));
        }

        #[test]
        fn test_temporal_date_explicit() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15") };
            let col = Evaluator::constant_value_of(&expr, DataType::Date, 2).unwrap();
            match col {
                ColumnValues::Date(values, validity) => {
                    assert_eq!(values.len(), 2);
                    assert_eq!(validity.len(), 2);
                    assert_eq!(validity[0], true);
                    assert_eq!(validity[1], true);
                    assert_eq!(values[0].to_string(), "2024-03-15");
                    assert_eq!(values[1].to_string(), "2024-03-15");
                }
                _ => panic!("Expected Date column"),
            }
        }

        #[test]
        fn test_temporal_datetime_explicit() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15T14:30:00") };
            let col = Evaluator::constant_value_of(&expr, DataType::DateTime, 1).unwrap();
            match col {
                ColumnValues::DateTime(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    assert_eq!(values[0].to_string(), "2024-03-15T14:30:00.000000000Z");
                }
                _ => panic!("Expected DateTime column"),
            }
        }

        #[test]
        fn test_temporal_time_explicit() {
            let expr = ConstantExpression::Temporal { span: make_span("14:30:00") };
            let col = Evaluator::constant_value_of(&expr, DataType::Time, 1).unwrap();
            match col {
                ColumnValues::Time(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    assert_eq!(values[0].to_string(), "14:30:00.000000000");
                }
                _ => panic!("Expected Time column"),
            }
        }

        #[test]
        fn test_temporal_interval_explicit() {
            let expr = ConstantExpression::Temporal { span: make_span("P1D") };
            let col = Evaluator::constant_value_of(&expr, DataType::Interval, 1).unwrap();
            match col {
                ColumnValues::Interval(values, validity) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(validity.len(), 1);
                    assert_eq!(validity[0], true);
                    assert_eq!(values[0].to_nanos(), 24 * 60 * 60 * 1_000_000_000);
                }
                _ => panic!("Expected Interval column"),
            }
        }

        #[test]
        fn test_temporal_wrong_type_date_as_time() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15") };
            let err = Evaluator::constant_value_of(&expr, DataType::Time, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_temporal_wrong_type_time_as_date() {
            let expr = ConstantExpression::Temporal { span: make_span("14:30:00") };
            let err = Evaluator::constant_value_of(&expr, DataType::Date, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_temporal_wrong_type_interval_as_datetime() {
            let expr = ConstantExpression::Temporal { span: make_span("P1D") };
            let err = Evaluator::constant_value_of(&expr, DataType::DateTime, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }

        #[test]
        fn test_temporal_mismatch_with_other_types() {
            let expr = ConstantExpression::Temporal { span: make_span("2024-03-15") };
            assert!(Evaluator::constant_value_of(&expr, DataType::Int1, 1).is_err());
            assert!(Evaluator::constant_value_of(&expr, DataType::Float8, 1).is_err());
            assert!(Evaluator::constant_value_of(&expr, DataType::Bool, 1).is_err());
            assert!(Evaluator::constant_value_of(&expr, DataType::Utf8, 1).is_err());
        }

        fn number_ok(expr: &str, data_type: DataType, row_count: usize, expected: ColumnValues) {
            let expr = ConstantExpression::Number { span: make_span(expr) };
            let result = Evaluator::constant_value_of(&expr, data_type, row_count).unwrap();
            assert_eq!(result, expected);
        }

        fn number_type_mismatch(expr: &str, data_type: DataType) {
            let expr = ConstantExpression::Number { span: make_span(expr) };
            let err = Evaluator::constant_value_of(&expr, data_type, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "TYPE_001");
        }
    }

    fn make_span(value: &str) -> Span {
        Span { column: SpanColumn(0), line: SpanLine(1), fragment: value.to_string() }
    }
}
