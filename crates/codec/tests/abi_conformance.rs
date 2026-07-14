// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Guards the FFI ABI against tag drift: reifydb-abi must stay dependency-free, so its
//! ColumnTypeCode enum carries its own discriminants. This test pins every discriminant to the
//! unified ValueKind byte. If it fails, the two tables diverged and every FFI plugin would decode
//! garbage silently; renumber ColumnTypeCode, never work around it.

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_codec::{
	column_type::{type_code_of, value_type_of},
	tag::ValueKind,
};

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

/// The wire carries a type code as a bare integer. Any decoder that reconstructs the enum must agree
/// with the discriminant the encoder wrote. A hand-rolled reverse table in the WASM marshaller once
/// drifted to a 0-based numbering, so `Bool` (1) decoded as `Float4`; this pins the round-trip.
#[test]
fn every_type_code_round_trips_through_its_discriminant() {
	for &code in ColumnTypeCode::ALL {
		assert_eq!(
			ColumnTypeCode::from_u8(code.byte()),
			Some(code),
			"ColumnTypeCode::{code:?} must decode back from the byte it encodes to"
		);
	}
}

/// Every code the ABI can put on the wire must map to a ValueType, so an empty column can carry its
/// type across the FFI instead of collapsing to a placeholder. `Undefined` is the one exception: it
/// is the absence of a type.
#[test]
fn every_type_code_maps_to_a_value_type() {
	for &code in ColumnTypeCode::ALL {
		let ty = value_type_of(code);
		if code == ColumnTypeCode::Undefined {
			assert_eq!(ty, None, "Undefined carries no value type");
			continue;
		}

		let ty = ty.unwrap_or_else(|| panic!("ColumnTypeCode::{code:?} has no ValueType mapping"));
		assert_eq!(
			type_code_of(&ty),
			code,
			"ValueType {ty:?} must map back to the ColumnTypeCode it came from"
		);
	}
}
