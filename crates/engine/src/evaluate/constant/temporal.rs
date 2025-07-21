// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{self, Error};
use crate::frame::ColumnValues;
use reifydb_core::diagnostic::temporal;
use reifydb_core::value::temporal::{parse_date, parse_datetime, parse_interval, parse_time};
use reifydb_core::{Span, Type};

pub struct TemporalParser;

impl TemporalParser {
    /// Parse temporal expression to a specific target type with detailed error handling
    pub fn from_temporal(
        span: impl Span,
        target: Type,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Self::parse_temporal_type(span, target, row_count)
    }

    /// Parse a temporal constant expression and create a column with the specified row count
    pub fn parse_temporal(span: impl Span, row_count: usize) -> evaluate::Result<ColumnValues> {
        let fragment = span.fragment();

        // Route based on character patterns
        if fragment.starts_with('P') || fragment.starts_with('p') {
            // Interval format (ISO 8601 duration)
            let interval = parse_interval(span).map_err(|e| evaluate::Error(e.diagnostic()))?;
            Ok(ColumnValues::interval(vec![interval; row_count]))
        } else if fragment.contains(':') && fragment.contains('-') {
            // DateTime format (contains both : and -)
            let datetime = parse_datetime(span).map_err(|e| evaluate::Error(e.diagnostic()))?;
            Ok(ColumnValues::datetime(vec![datetime; row_count]))
        } else if fragment.contains('-') {
            // Date format with - separators
            let date = parse_date(span).map_err(|e| evaluate::Error(e.diagnostic()))?;
            Ok(ColumnValues::date(vec![date; row_count]))
        } else if fragment.contains(':') {
            // Time format (contains :)
            let time = parse_time(span).map_err(|e| evaluate::Error(e.diagnostic()))?;
            Ok(ColumnValues::time(vec![time; row_count]))
        } else {
            // Unrecognized pattern
            Err(Error(temporal::unrecognized_temporal_pattern(span.to_owned())))
        }
    }

    /// Parse temporal to specific target type with detailed error handling
    pub fn parse_temporal_type(
        span: impl Span,
        target: Type,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        use reifydb_core::diagnostic::cast;

        match target {
            Type::Date => {
                let date = parse_date(span.clone())
                    .map_err(|e| Error(cast::invalid_temporal(span.clone().to_owned(), Type::Date, e.0)))?;
                Ok(ColumnValues::date(vec![date; row_count]))
            }
            Type::DateTime => {
                let datetime = parse_datetime(span.clone())
                    .map_err(|e| Error(cast::invalid_temporal(span.clone().to_owned(), Type::DateTime, e.0)))?;
                Ok(ColumnValues::datetime(vec![datetime; row_count]))
            }
            Type::Time => {
                let time = parse_time(span.clone())
                    .map_err(|e| Error(cast::invalid_temporal(span.clone().to_owned(), Type::Time, e.0)))?;
                Ok(ColumnValues::time(vec![time; row_count]))
            }
            Type::Interval => {
                let interval = parse_interval(span.clone())
                    .map_err(|e| Error(cast::invalid_temporal(span.clone().to_owned(), Type::Interval, e.0)))?;
                Ok(ColumnValues::interval(vec![interval; row_count]))
            }
            _ => Err(Error(cast::unsupported_cast(span.to_owned(), Type::DateTime, target))),
        }
    }
}