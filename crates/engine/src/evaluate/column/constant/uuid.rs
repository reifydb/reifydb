// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::ColumnData;
use reifydb_type::{Fragment, Type, diagnostic::cast, parse_uuid4, parse_uuid7, return_error};

pub(crate) struct UuidParser;

impl UuidParser {
	/// Parse text to a specific UUID target type with detailed error
	/// handling
	pub(crate) fn from_text<'a>(
		fragment: impl Into<Fragment>,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into();
		match target {
			Type::Uuid4 => Self::parse_uuid4(fragment, row_count),
			Type::Uuid7 => Self::parse_uuid7(fragment, row_count),
			_ => return_error!(cast::unsupported_cast(fragment, Type::Utf8, target)),
		}
	}

	fn parse_uuid4<'a>(fragment: impl Into<Fragment>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into();
		match parse_uuid4(fragment.clone()) {
			Ok(uuid) => Ok(ColumnData::uuid4(vec![uuid; row_count])),
			Err(err) => {
				return_error!(cast::invalid_uuid(fragment, Type::Uuid4, err.diagnostic()))
			}
		}
	}

	fn parse_uuid7<'a>(fragment: impl Into<Fragment>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into();
		match parse_uuid7(fragment.clone()) {
			Ok(uuid) => Ok(ColumnData::uuid7(vec![uuid; row_count])),
			Err(err) => {
				return_error!(cast::invalid_uuid(fragment, Type::Uuid7, err.diagnostic()))
			}
		}
	}
}
