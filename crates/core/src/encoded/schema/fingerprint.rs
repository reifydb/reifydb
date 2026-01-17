// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema fingerprint computation for content-addressable storage.
//!
//! The fingerprint is a deterministic hash of the schema's canonical representation,
//! ensuring that identical schemas always produce the same fingerprint regardless
//! of when or where they are created.

use std::ops::Deref;

use reifydb_hash::{Hash64, xxh::xxh3_64};
use serde::{Deserialize, Serialize};

use crate::schema::SchemaField;

/// A fingerprint that uniquely identifies a schema layout.
///
/// This is an 8-byte hash stored in the header of every encoded row,
/// allowing the schema to be identified without external metadata.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaFingerprint(pub Hash64);

impl Deref for SchemaFingerprint {
	type Target = u64;

	fn deref(&self) -> &Self::Target {
		&self.0.0
	}
}

impl SchemaFingerprint {
	/// Create a new schema fingerprint from a u64 value.
	#[inline]
	pub const fn new(value: u64) -> Self {
		Self(Hash64(value))
	}

	/// Create a zero/empty fingerprint.
	#[inline]
	pub const fn zero() -> Self {
		Self(Hash64(0))
	}

	/// Get the underlying u64 value.
	#[inline]
	pub const fn as_u64(&self) -> u64 {
		self.0.0
	}

	/// Convert to little-endian bytes.
	#[inline]
	pub const fn to_le_bytes(&self) -> [u8; 8] {
		self.0.0.to_le_bytes()
	}

	/// Create from little-endian bytes.
	#[inline]
	pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
		Self(Hash64(u64::from_le_bytes(bytes)))
	}
}

impl From<Hash64> for SchemaFingerprint {
	fn from(hash: Hash64) -> Self {
		Self(hash)
	}
}

impl From<SchemaFingerprint> for Hash64 {
	fn from(fp: SchemaFingerprint) -> Self {
		fp.0
	}
}

impl From<u64> for SchemaFingerprint {
	fn from(value: u64) -> Self {
		Self(Hash64(value))
	}
}

/// Compute a deterministic fingerprint for a schema based on its fields.
///
/// The fingerprint is computed by hashing a canonical binary representation
/// of the fields. This ensures:
/// - Same fields → same fingerprint (deterministic)
/// - Different fields → different fingerprint (collision-resistant)
///
/// The canonical representation includes:
/// - Number of fields (u16)
/// - For each field:
///   - Field name length (u16) + name bytes (UTF-8)
///   - Base type (u8)
///   - Constraint type (u8)
///   - Constraint param1 (u32)
///   - Constraint param2 (u32)
pub fn compute_fingerprint(fields: &[SchemaField]) -> SchemaFingerprint {
	// Estimate buffer size: 2 bytes for count + ~42 bytes per field average
	let estimated_size = 2 + fields.len() * 42;
	let mut buffer = Vec::with_capacity(estimated_size);

	// Write field count as u16 (max 65535 fields)
	let field_count = fields.len() as u16;
	buffer.extend_from_slice(&field_count.to_le_bytes());

	// Write each field in canonical order
	for field in fields {
		// Write name length and bytes
		let name_bytes = field.name.as_bytes();
		let name_len = name_bytes.len() as u16;
		buffer.extend_from_slice(&name_len.to_le_bytes());
		buffer.extend_from_slice(name_bytes);

		// Write constraint info (base type + constraint type + params)
		let ffi = field.constraint.to_ffi();
		buffer.push(ffi.base_type);
		buffer.push(ffi.constraint_type);
		buffer.extend_from_slice(&ffi.constraint_param1.to_le_bytes());
		buffer.extend_from_slice(&ffi.constraint_param2.to_le_bytes());
	}

	SchemaFingerprint(xxh3_64(&buffer))
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::{
		constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
		r#type::Type,
	};

	use super::*;

	fn make_field(name: &str, field_type: Type) -> SchemaField {
		SchemaField {
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(field_type),
			offset: 0,
			size: 0,
			align: 0,
		}
	}

	fn make_constrained_field(name: &str, constraint: TypeConstraint) -> SchemaField {
		SchemaField {
			name: name.to_string(),
			constraint,
			offset: 0,
			size: 0,
			align: 0,
		}
	}

	#[test]
	fn test_fingerprint_deterministic() {
		let fields1 = vec![make_field("a", Type::Int4), make_field("b", Type::Utf8)];

		let fields2 = vec![make_field("a", Type::Int4), make_field("b", Type::Utf8)];

		assert_eq!(compute_fingerprint(&fields1), compute_fingerprint(&fields2));
	}

	#[test]
	fn test_fingerprint_different_names() {
		let fields1 = vec![make_field("a", Type::Int4)];
		let fields2 = vec![make_field("b", Type::Int4)];

		assert_ne!(compute_fingerprint(&fields1), compute_fingerprint(&fields2));
	}

	#[test]
	fn test_fingerprint_different_types() {
		let fields1 = vec![make_field("a", Type::Int4)];
		let fields2 = vec![make_field("a", Type::Int8)];

		assert_ne!(compute_fingerprint(&fields1), compute_fingerprint(&fields2));
	}

	#[test]
	fn test_fingerprint_different_order() {
		let fields1 = vec![make_field("a", Type::Int4), make_field("b", Type::Utf8)];

		let fields2 = vec![make_field("b", Type::Utf8), make_field("a", Type::Int4)];

		assert_ne!(compute_fingerprint(&fields1), compute_fingerprint(&fields2));
	}

	#[test]
	fn test_fingerprint_empty_schema() {
		let fields: Vec<SchemaField> = vec![];
		// Should not panic and should produce a valid hash
		let fp = compute_fingerprint(&fields);
		assert_ne!(*fp, 0);
	}

	// ==================== Constraint Tests ====================

	// --- Utf8 with MaxBytes constraint ---

	#[test]
	fn test_fingerprint_utf8_constrained_vs_unconstrained() {
		let unconstrained = vec![make_field("text", Type::Utf8)];
		let constrained = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
		)];

		assert_ne!(
			compute_fingerprint(&unconstrained),
			compute_fingerprint(&constrained),
			"Utf8 unconstrained should differ from Utf8(255)"
		);
	}

	#[test]
	fn test_fingerprint_utf8_same_constraint_deterministic() {
		let fields1 = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
		)];
		let fields2 = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
		)];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Utf8(100) should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_utf8_different_max_bytes() {
		let small = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(50))),
		)];
		let large = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(500))),
		)];

		assert_ne!(
			compute_fingerprint(&small),
			compute_fingerprint(&large),
			"Utf8(50) should differ from Utf8(500)"
		);
	}

	#[test]
	fn test_fingerprint_int_constrained_vs_unconstrained() {
		let unconstrained = vec![make_field("num", Type::Int)];
		let constrained = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(8))),
		)];

		assert_ne!(
			compute_fingerprint(&unconstrained),
			compute_fingerprint(&constrained),
			"Int unconstrained should differ from Int(8)"
		);
	}

	#[test]
	fn test_fingerprint_int_same_constraint_deterministic() {
		let fields1 = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(16))),
		)];
		let fields2 = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(16))),
		)];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Int(16) should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_int_different_max_bytes() {
		let small = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(4))),
		)];
		let large = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(32))),
		)];

		assert_ne!(
			compute_fingerprint(&small),
			compute_fingerprint(&large),
			"Int(4) should differ from Int(32)"
		);
	}

	#[test]
	fn test_fingerprint_uint_constrained_vs_unconstrained() {
		let unconstrained = vec![make_field("num", Type::Uint)];
		let constrained = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(8))),
		)];

		assert_ne!(
			compute_fingerprint(&unconstrained),
			compute_fingerprint(&constrained),
			"Uint unconstrained should differ from Uint(8)"
		);
	}

	#[test]
	fn test_fingerprint_uint_same_constraint_deterministic() {
		let fields1 = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(64))),
		)];
		let fields2 = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(64))),
		)];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Uint(64) should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_uint_different_max_bytes() {
		let small = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(2))),
		)];
		let large = vec![make_constrained_field(
			"num",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(128))),
		)];

		assert_ne!(
			compute_fingerprint(&small),
			compute_fingerprint(&large),
			"Uint(2) should differ from Uint(128)"
		);
	}

	#[test]
	fn test_fingerprint_blob_constrained_vs_unconstrained() {
		let unconstrained = vec![make_field("data", Type::Blob)];
		let constrained = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
		)];

		assert_ne!(
			compute_fingerprint(&unconstrained),
			compute_fingerprint(&constrained),
			"Blob unconstrained should differ from Blob(1024)"
		);
	}

	#[test]
	fn test_fingerprint_blob_same_constraint_deterministic() {
		let fields1 = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(4096))),
		)];
		let fields2 = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(4096))),
		)];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Blob(4096) should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_blob_different_max_bytes() {
		let small = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(256))),
		)];
		let large = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(65536))),
		)];

		assert_ne!(
			compute_fingerprint(&small),
			compute_fingerprint(&large),
			"Blob(256) should differ from Blob(65536)"
		);
	}

	#[test]
	fn test_fingerprint_decimal_constrained_vs_unconstrained() {
		let unconstrained = vec![make_field("amount", Type::Decimal)];
		let constrained = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
			),
		)];

		assert_ne!(
			compute_fingerprint(&unconstrained),
			compute_fingerprint(&constrained),
			"Decimal unconstrained should differ from Decimal(10,2)"
		);
	}

	#[test]
	fn test_fingerprint_decimal_same_constraint_deterministic() {
		let fields1 = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(18), Scale::new(6)),
			),
		)];
		let fields2 = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(18), Scale::new(6)),
			),
		)];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Decimal(18,6) should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_decimal_different_precision() {
		let low_precision = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(5), Scale::new(2)),
			),
		)];
		let high_precision = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(38), Scale::new(2)),
			),
		)];

		assert_ne!(
			compute_fingerprint(&low_precision),
			compute_fingerprint(&high_precision),
			"Decimal(5,2) should differ from Decimal(38,2)"
		);
	}

	#[test]
	fn test_fingerprint_decimal_different_scale() {
		let low_scale = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(0)),
			),
		)];
		let high_scale = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(8)),
			),
		)];

		assert_ne!(
			compute_fingerprint(&low_scale),
			compute_fingerprint(&high_scale),
			"Decimal(10,0) should differ from Decimal(10,8)"
		);
	}

	#[test]
	fn test_fingerprint_decimal_different_precision_and_scale() {
		let fields1 = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
			),
		)];
		let fields2 = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(15), Scale::new(4)),
			),
		)];

		assert_ne!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Decimal(10,2) should differ from Decimal(15,4)"
		);
	}

	#[test]
	fn test_fingerprint_different_types_same_max_bytes() {
		// Same MaxBytes value but different base types should produce different fingerprints
		let utf8 = vec![make_constrained_field(
			"field",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
		)];
		let blob = vec![make_constrained_field(
			"field",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(100))),
		)];
		let int = vec![make_constrained_field(
			"field",
			TypeConstraint::with_constraint(Type::Int, Constraint::MaxBytes(MaxBytes::new(100))),
		)];
		let uint = vec![make_constrained_field(
			"field",
			TypeConstraint::with_constraint(Type::Uint, Constraint::MaxBytes(MaxBytes::new(100))),
		)];

		let fp_utf8 = compute_fingerprint(&utf8);
		let fp_blob = compute_fingerprint(&blob);
		let fp_int = compute_fingerprint(&int);
		let fp_uint = compute_fingerprint(&uint);

		assert_ne!(fp_utf8, fp_blob, "Utf8(100) should differ from Blob(100)");
		assert_ne!(fp_utf8, fp_int, "Utf8(100) should differ from Int(100)");
		assert_ne!(fp_utf8, fp_uint, "Utf8(100) should differ from Uint(100)");
		assert_ne!(fp_blob, fp_int, "Blob(100) should differ from Int(100)");
		assert_ne!(fp_blob, fp_uint, "Blob(100) should differ from Uint(100)");
		assert_ne!(fp_int, fp_uint, "Int(100) should differ from Uint(100)");
	}

	#[test]
	fn test_fingerprint_multiple_constrained_fields() {
		let fields1 = vec![
			make_constrained_field(
				"name",
				TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
			),
			make_constrained_field(
				"price",
				TypeConstraint::with_constraint(
					Type::Decimal,
					Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
				),
			),
			make_constrained_field(
				"data",
				TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
			),
		];

		let fields2 = vec![
			make_constrained_field(
				"name",
				TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
			),
			make_constrained_field(
				"price",
				TypeConstraint::with_constraint(
					Type::Decimal,
					Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
				),
			),
			make_constrained_field(
				"data",
				TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(1024))),
			),
		];

		assert_eq!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Identical multi-field constrained schemas should produce same fingerprint"
		);
	}

	#[test]
	fn test_fingerprint_multiple_fields_one_constraint_differs() {
		let fields1 = vec![
			make_constrained_field(
				"name",
				TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
			),
			make_constrained_field(
				"price",
				TypeConstraint::with_constraint(
					Type::Decimal,
					Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
				),
			),
		];

		let fields2 = vec![
			make_constrained_field(
				"name",
				TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(255))),
			),
			make_constrained_field(
				"price",
				TypeConstraint::with_constraint(
					Type::Decimal,
					Constraint::PrecisionScale(Precision::new(10), Scale::new(4)), /* Different scale */
				),
			),
		];

		assert_ne!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Schemas differing only in one constraint should have different fingerprints"
		);
	}

	#[test]
	fn test_fingerprint_mixed_constrained_and_unconstrained() {
		let fields1 = vec![
			make_field("id", Type::Int8),
			make_constrained_field(
				"name",
				TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
			),
			make_field("active", Type::Boolean),
		];

		let fields2 = vec![
			make_field("id", Type::Int8),
			make_field("name", Type::Utf8), // Unconstrained
			make_field("active", Type::Boolean),
		];

		assert_ne!(
			compute_fingerprint(&fields1),
			compute_fingerprint(&fields2),
			"Mixed constrained/unconstrained should differ from all unconstrained"
		);
	}

	// --- Edge case constraint values ---

	#[test]
	fn test_fingerprint_max_bytes_edge_values() {
		let min_value = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(1))),
		)];
		let max_value = vec![make_constrained_field(
			"data",
			TypeConstraint::with_constraint(Type::Blob, Constraint::MaxBytes(MaxBytes::new(u32::MAX))),
		)];

		assert_ne!(
			compute_fingerprint(&min_value),
			compute_fingerprint(&max_value),
			"Blob(1) should differ from Blob(MAX)"
		);
	}

	#[test]
	fn test_fingerprint_decimal_edge_precision_scale() {
		let min_precision = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(1), Scale::new(0)),
			),
		)];
		let max_precision = vec![make_constrained_field(
			"amount",
			TypeConstraint::with_constraint(
				Type::Decimal,
				Constraint::PrecisionScale(Precision::new(255), Scale::new(255)),
			),
		)];

		assert_ne!(
			compute_fingerprint(&min_precision),
			compute_fingerprint(&max_precision),
			"Decimal(1,0) should differ from Decimal(255,255)"
		);
	}

	#[test]
	fn test_fingerprint_adjacent_max_bytes_values() {
		// Test that even adjacent values produce different fingerprints
		let value_99 = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(99))),
		)];
		let value_100 = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(100))),
		)];
		let value_101 = vec![make_constrained_field(
			"text",
			TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(101))),
		)];

		let fp_99 = compute_fingerprint(&value_99);
		let fp_100 = compute_fingerprint(&value_100);
		let fp_101 = compute_fingerprint(&value_101);

		assert_ne!(fp_99, fp_100, "Utf8(99) should differ from Utf8(100)");
		assert_ne!(fp_100, fp_101, "Utf8(100) should differ from Utf8(101)");
		assert_ne!(fp_99, fp_101, "Utf8(99) should differ from Utf8(101)");
	}
}
