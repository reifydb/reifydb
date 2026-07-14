// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Guards the FFI ABI against tag drift: reifydb-abi must stay dependency-free, so its
//! ColumnTypeCode enum carries its own discriminants. This test pins every discriminant to the
//! unified ValueKind byte. If it fails, the two tables diverged and every FFI plugin would decode
//! garbage silently; renumber ColumnTypeCode, never work around it.

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_codec::tag::ValueKind;

#[test]
fn column_type_code_discriminants_match_value_kind_bytes() {
	let pairs = [
		(ColumnTypeCode::Undefined, ValueKind::None),
		(ColumnTypeCode::Bool, ValueKind::Boolean),
		(ColumnTypeCode::Float4, ValueKind::Float4),
		(ColumnTypeCode::Float8, ValueKind::Float8),
		(ColumnTypeCode::Int1, ValueKind::Int1),
		(ColumnTypeCode::Int2, ValueKind::Int2),
		(ColumnTypeCode::Int4, ValueKind::Int4),
		(ColumnTypeCode::Int8, ValueKind::Int8),
		(ColumnTypeCode::Int16, ValueKind::Int16),
		(ColumnTypeCode::Utf8, ValueKind::Utf8),
		(ColumnTypeCode::Uint1, ValueKind::Uint1),
		(ColumnTypeCode::Uint2, ValueKind::Uint2),
		(ColumnTypeCode::Uint4, ValueKind::Uint4),
		(ColumnTypeCode::Uint8, ValueKind::Uint8),
		(ColumnTypeCode::Uint16, ValueKind::Uint16),
		(ColumnTypeCode::Date, ValueKind::Date),
		(ColumnTypeCode::DateTime, ValueKind::DateTime),
		(ColumnTypeCode::Time, ValueKind::Time),
		(ColumnTypeCode::Duration, ValueKind::Duration),
		(ColumnTypeCode::IdentityId, ValueKind::IdentityId),
		(ColumnTypeCode::Uuid4, ValueKind::Uuid4),
		(ColumnTypeCode::Uuid7, ValueKind::Uuid7),
		(ColumnTypeCode::Blob, ValueKind::Blob),
		(ColumnTypeCode::Int, ValueKind::Int),
		(ColumnTypeCode::Uint, ValueKind::Uint),
		(ColumnTypeCode::Decimal, ValueKind::Decimal),
		(ColumnTypeCode::Any, ValueKind::Any),
		(ColumnTypeCode::DictionaryId, ValueKind::DictionaryId),
		(ColumnTypeCode::Vector, ValueKind::Vector),
	];
	for (code, kind) in pairs {
		assert_eq!(
			code as u8,
			kind.byte(),
			"ColumnTypeCode::{code:?} must carry the unified tag byte of ValueKind::{kind:?}"
		);
	}
}
