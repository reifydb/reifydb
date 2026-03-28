// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::TypeError,
	fragment::Fragment,
	value::{
		r#type::Type,
		uuid::parse::{parse_identity_id, parse_uuid4, parse_uuid7},
	},
};

use crate::{Result, error::CastError};

pub(crate) struct UuidParser;

impl UuidParser {
	/// Parse text to a specific UUID target type with detailed error
	/// handling
	pub(crate) fn from_text<'a>(
		fragment: impl Into<Fragment>,
		target: Type,
		row_count: usize,
	) -> Result<ColumnData> {
		let fragment = fragment.into();
		match target {
			Type::Uuid4 => Self::parse_uuid4(fragment, row_count),
			Type::Uuid7 => Self::parse_uuid7(fragment, row_count),
			Type::IdentityId => Self::parse_identity_id(fragment, row_count),
			_ => {
				return Err(TypeError::UnsupportedCast {
					from: Type::Utf8,
					to: target,
					fragment,
				}
				.into());
			}
		}
	}

	fn parse_uuid4<'a>(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnData> {
		let fragment = fragment.into();
		match parse_uuid4(fragment.clone()) {
			Ok(uuid) => Ok(ColumnData::uuid4(vec![uuid; row_count])),
			Err(err) => {
				return Err(CastError::InvalidUuid {
					fragment,
					target: Type::Uuid4,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_uuid7<'a>(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnData> {
		let fragment = fragment.into();
		match parse_uuid7(fragment.clone()) {
			Ok(uuid) => Ok(ColumnData::uuid7(vec![uuid; row_count])),
			Err(err) => {
				return Err(CastError::InvalidUuid {
					fragment,
					target: Type::Uuid7,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_identity_id<'a>(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnData> {
		let fragment = fragment.into();
		match parse_identity_id(fragment.clone()) {
			Ok(id) => Ok(ColumnData::identity_id(vec![id; row_count])),
			Err(err) => {
				return Err(CastError::InvalidUuid {
					fragment,
					target: Type::IdentityId,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}
}
