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
///   - Field type (u8)
pub fn compute_fingerprint(fields: &[SchemaField]) -> SchemaFingerprint {
	// Estimate buffer size: 2 bytes for count + ~32 bytes per field average
	let estimated_size = 2 + fields.len() * 32;
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

		// Write type as u8
		buffer.push(field.field_type.to_u8());
	}

	SchemaFingerprint(xxh3_64(&buffer))
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::*;

	fn make_field(name: &str, field_type: Type) -> SchemaField {
		SchemaField {
			name: name.to_string(),
			field_type,
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
}
