// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	IntoFragment, OwnedFragment, Type,
	result::error::diagnostic::cast,
	return_error,
	value::uuid::parse::{parse_uuid4, parse_uuid7},
};

use crate::columnar::ColumnData;

pub(crate) struct UuidParser;

impl UuidParser {
	/// Parse text to a specific UUID target type with detailed error
	/// handling
	pub(crate) fn from_text(
		fragment: impl IntoFragment,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match target {
			Type::Uuid4 => Self::parse_uuid4(&fragment, row_count),
			Type::Uuid7 => Self::parse_uuid7(&fragment, row_count),
			_ => return_error!(cast::unsupported_cast(
				fragment.clone(),
				Type::Utf8,
				target
			)),
		}
	}

	fn parse_uuid4(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		match parse_uuid4(fragment.clone()) {
			Ok(uuid) => {
				Ok(ColumnData::uuid4(vec![uuid; row_count]))
			}
			Err(err) => {
				return_error!(cast::invalid_uuid(
					fragment.clone(),
					Type::Uuid4,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_uuid7(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		match parse_uuid7(fragment.clone()) {
			Ok(uuid) => {
				Ok(ColumnData::uuid7(vec![uuid; row_count]))
			}
			Err(err) => {
				return_error!(cast::invalid_uuid(
					fragment.clone(),
					Type::Uuid7,
					err.diagnostic()
				))
			}
		}
	}
}
