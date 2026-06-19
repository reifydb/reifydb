// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	error::TypeError,
	fragment::Fragment,
	value::{
		uuid::parse::{parse_identity_id, parse_uuid4, parse_uuid7},
		value_type::ValueType,
	},
};

use crate::{Result, error::CastError};

pub(crate) struct UuidParser;

impl UuidParser {
	pub(crate) fn from_text(
		fragment: impl Into<Fragment>,
		target: ValueType,
		row_count: usize,
	) -> Result<ColumnBuffer> {
		let fragment = fragment.into();
		match target {
			ValueType::Uuid4 => Self::parse_uuid4(fragment, row_count),
			ValueType::Uuid7 => Self::parse_uuid7(fragment, row_count),
			ValueType::IdentityId => Self::parse_identity_id(fragment, row_count),
			_ => Err(TypeError::UnsupportedCast {
				from: ValueType::Utf8,
				to: target,
				fragment,
			}
			.into()),
		}
	}

	fn parse_uuid4(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnBuffer> {
		let fragment = fragment.into();
		match parse_uuid4(fragment.clone()) {
			Ok(uuid) => Ok(ColumnBuffer::uuid4(vec![uuid; row_count])),
			Err(err) => Err(CastError::InvalidUuid {
				fragment,
				target: ValueType::Uuid4,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_uuid7(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnBuffer> {
		let fragment = fragment.into();
		match parse_uuid7(fragment.clone()) {
			Ok(uuid) => Ok(ColumnBuffer::uuid7(vec![uuid; row_count])),
			Err(err) => Err(CastError::InvalidUuid {
				fragment,
				target: ValueType::Uuid7,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_identity_id(fragment: impl Into<Fragment>, row_count: usize) -> Result<ColumnBuffer> {
		let fragment = fragment.into();
		match parse_identity_id(fragment.clone()) {
			Ok(id) => Ok(ColumnBuffer::identity_id(vec![id; row_count])),
			Err(err) => Err(CastError::InvalidUuid {
				fragment,
				target: ValueType::IdentityId,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}
}
