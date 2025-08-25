// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	IntoFragment, Type,
	result::error::diagnostic::cast,
	return_error,
	value::uuid::parse::{parse_uuid4, parse_uuid7},
};

use crate::columnar::ColumnData;

pub(crate) struct UuidParser;

impl UuidParser {
	/// Parse text to a specific UUID target type with detailed error
	/// handling
	pub(crate) fn from_text<'a>(
		fragment: impl IntoFragment<'a>,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match target {
			Type::Uuid4 => Self::parse_uuid4(fragment, row_count),
			Type::Uuid7 => Self::parse_uuid7(fragment, row_count),
			_ => return_error!(cast::unsupported_cast(
				fragment,
				Type::Utf8,
				target
			)),
		}
	}

	fn parse_uuid4<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_uuid4(&fragment) {
			Ok(uuid) => {
				Ok(ColumnData::uuid4(vec![uuid; row_count]))
			}
			Err(err) => {
				return_error!(cast::invalid_uuid(
					fragment,
					Type::Uuid4,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_uuid7<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_uuid7(&fragment) {
			Ok(uuid) => {
				Ok(ColumnData::uuid7(vec![uuid; row_count]))
			}
			Err(err) => {
				return_error!(cast::invalid_uuid(
					fragment,
					Type::Uuid7,
					err.diagnostic()
				))
			}
		}
	}
}
