// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::columnar::ColumnData;
use reifydb_core::result::error::diagnostic::cast;
use reifydb_core::value::uuid::parse::{parse_uuid4, parse_uuid7};
use reifydb_core::{Span, Type, return_error};

pub(crate) struct UuidParser;

impl UuidParser {
    /// Parse text to a specific UUID target type with detailed error handling
    pub(crate) fn from_text(
        span: impl Span,
        target: Type,
        row_count: usize,
    ) -> crate::Result<ColumnData> {
        match target {
            Type::Uuid4 => Self::parse_uuid4(span, row_count),
            Type::Uuid7 => Self::parse_uuid7(span, row_count),
            _ => return_error!(cast::unsupported_cast(span.to_owned(), Type::Utf8, target)),
        }
    }

    fn parse_uuid4(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        match parse_uuid4(span.clone()) {
            Ok(uuid) => Ok(ColumnData::uuid4(vec![uuid; row_count])),
            Err(err) => {
                return_error!(cast::invalid_uuid(span.to_owned(), Type::Uuid4, err.diagnostic()))
            }
        }
    }

    fn parse_uuid7(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        match parse_uuid7(span.clone()) {
            Ok(uuid) => Ok(ColumnData::uuid7(vec![uuid; row_count])),
            Err(err) => {
                return_error!(cast::invalid_uuid(span.to_owned(), Type::Uuid7, err.diagnostic()))
            }
        }
    }
}
