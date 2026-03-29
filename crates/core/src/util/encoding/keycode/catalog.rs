// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{Result, value::dictionary::DictionaryId};

use super::{deserialize, serialize};
use crate::{
	interface::catalog::{
		id::{IndexId, PrimaryKeyId, RingBufferId, SeriesId, TableId, ViewId},
		shape::ShapeId,
		vtable::VTableId,
	},
	return_internal_error,
};

/// Serialize a ShapeId for use in database keys
/// Returns [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View, 0x03 for TableVirtual, 0x04 for RingBuffer, 0x06 for Dictionary, 0x07 for Series
pub fn serialize_shape_id(shape: &ShapeId) -> Vec<u8> {
	let mut result = Vec::with_capacity(9);
	match shape {
		ShapeId::Table(TableId(id)) => {
			result.push(0x01);
			result.extend(&serialize(id));
		}
		ShapeId::View(ViewId(id)) => {
			result.push(0x02);
			result.extend(&serialize(id));
		}
		ShapeId::TableVirtual(VTableId(id)) => {
			result.push(0x03);
			result.extend(&serialize(id));
		}
		ShapeId::RingBuffer(RingBufferId(id)) => {
			result.push(0x04);
			result.extend(&serialize(id));
		}
		ShapeId::Dictionary(DictionaryId(id)) => {
			result.push(0x06);
			result.extend(&serialize(id));
		}
		ShapeId::Series(SeriesId(id)) => {
			result.push(0x07);
			result.extend(&serialize(id));
		}
	}
	result
}

/// Deserialize a ShapeId from database key bytes
/// Expects [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View, 0x03 for TableVirtual, 0x04 for RingBuffer, 0x06 for Dictionary, 0x07 for Series
pub fn deserialize_shape_id(bytes: &[u8]) -> Result<ShapeId> {
	if bytes.len() != 9 {
		return_internal_error!("Invalid ShapeId encoding: expected 9 bytes, got {}", bytes.len());
	}

	let type_byte = bytes[0];
	let id: u64 = deserialize(&bytes[1..9])?;

	match type_byte {
		0x01 => Ok(ShapeId::Table(TableId(id))),
		0x02 => Ok(ShapeId::View(ViewId(id))),
		0x03 => Ok(ShapeId::TableVirtual(VTableId(id))),
		0x04 => Ok(ShapeId::RingBuffer(RingBufferId(id))),
		0x06 => Ok(ShapeId::Dictionary(DictionaryId(id))),
		0x07 => Ok(ShapeId::Series(SeriesId(id))),
		_ => return_internal_error!("Invalid ShapeId type byte: 0x{:02x}.", type_byte),
	}
}

/// Serialize an IndexId for use in database keys
/// Returns [type_byte, ...id_bytes]
pub fn serialize_index_id(index: &IndexId) -> Vec<u8> {
	let mut result = Vec::with_capacity(9);
	match index {
		IndexId::Primary(PrimaryKeyId(id)) => {
			result.push(0x01);
			result.extend(&serialize(id));
		} // Future: Secondary, Unique, etc.
	}
	result
}

/// Deserialize an IndexId from database key bytes
/// Expects [type_byte, ...id_bytes]
pub fn deserialize_index_id(bytes: &[u8]) -> Result<IndexId> {
	if bytes.len() != 9 {
		return_internal_error!("Invalid IndexId encoding: expected 9 bytes, got {}", bytes.len());
	}

	let type_byte = bytes[0];
	let id: u64 = deserialize(&bytes[1..9])?;

	match type_byte {
		0x01 => Ok(IndexId::Primary(PrimaryKeyId(id))),
		// Future: 0x02 => Ok(IndexId::Secondary(...)), etc.
		_ => return_internal_error!("Invalid IndexId type byte: 0x{:02x}.", type_byte),
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::util::encoding::keycode::serialize;

	#[test]
	fn test_shape_id_ordering() {
		// Test that larger IDs encode to smaller byte sequences
		// (descending order)
		let primitive1 = ShapeId::table(1);
		let primitive2 = ShapeId::table(2);
		let primitive100 = ShapeId::table(100);
		let primitive200 = ShapeId::table(200);

		let bytes1 = serialize_shape_id(&primitive1);
		let bytes2 = serialize_shape_id(&primitive2);
		let bytes100 = serialize_shape_id(&primitive100);
		let bytes200 = serialize_shape_id(&primitive200);

		// In descending order, larger values should have smaller byte
		// representations
		assert!(bytes2 < bytes1, "shape(2) should be < shape(1) in bytes");
		assert!(bytes200 < bytes100, "shape(200) should be < shape(100) in bytes");
		assert!(bytes100 < bytes2, "shape(100) should be < shape(2) in bytes");
	}

	#[test]
	fn test_range_boundaries() {
		// Test range boundary creation for tables
		let primitive10 = ShapeId::table(10);
		let primitive9 = primitive10.prev();

		let bytes10 = serialize_shape_id(&primitive10);
		let bytes9 = serialize_shape_id(&primitive9);

		// In descending order, shape(9) > shape(10)
		assert!(bytes9 > bytes10, "shape(9) should be > shape(10) in bytes");

		// Test with views
		let view10 = ShapeId::view(10);
		let view9 = view10.prev();

		let vbytes10 = serialize_shape_id(&view10);
		let vbytes9 = serialize_shape_id(&view9);

		// In descending order, view(9) > view(10)
		assert!(vbytes9 > vbytes10, "view(9) should be > view(10) in bytes");

		// Test with virtual tables
		let virtual10 = ShapeId::vtable(10);
		let virtual9 = virtual10.prev();

		let tvbytes10 = serialize_shape_id(&virtual10);
		let tvbytes9 = serialize_shape_id(&virtual9);

		// In descending order, vtable(9) > vtable(10)
		assert!(tvbytes9 > tvbytes10, "vtable(9) should be > vtable(10) in bytes");

		// Check that view, table, and vtable with same ID encode
		// differently
		assert_ne!(bytes10, vbytes10, "table(10) should != view(10)");
		assert_ne!(bytes10, tvbytes10, "table(10) should != vtable(10)");
		assert_ne!(vbytes10, tvbytes10, "view(10) should != vtable(10)");
		assert_eq!(bytes10[0], 0x01, "table type byte should be 0x01");
		assert_eq!(vbytes10[0], 0x02, "view type byte should be 0x02");
		assert_eq!(tvbytes10[0], 0x03, "vtable type byte should be 0x03");

		// Simulate what happens with encoded keys
		let row_key_10_100 = vec![0xFE, 0xFC]; // version, kind
		let mut key1 = row_key_10_100.clone();
		key1.extend(&bytes10);
		key1.extend(&serialize(&100u64)); // encoded 100

		let mut key2 = row_key_10_100.clone();
		key2.extend(&bytes10);
		key2.extend(&serialize(&200u64)); // encoded 200

		let mut end_key = vec![0xFE, 0xFC];
		end_key.extend(&bytes9);

		// Range check assertions
		assert!(key1 >= bytes10, "key1 should be >= start(primitive10)");
		assert!(key1 < end_key, "key1 should be < end(primitive9)");
		assert!(key2 >= bytes10, "key2 should be >= start(primitive10)");
		assert!(key2 < end_key, "key2 should be < end(primitive9)");
	}

	#[test]
	fn test_vtable_serialization() {
		// Test basic serialization/deserialization
		let virtual_primitive = ShapeId::vtable(42);
		let bytes = serialize_shape_id(&virtual_primitive);
		let deserialized = deserialize_shape_id(&bytes).unwrap();
		assert_eq!(virtual_primitive, deserialized);

		// Test that type byte is 0x03
		assert_eq!(bytes[0], 0x03);

		// Test with VTableId directly
		let virtual_id = VTableId(123);
		let primitive_from_id = ShapeId::from(virtual_id);
		let bytes_from_id = serialize_shape_id(&primitive_from_id);
		let deserialized_id = deserialize_shape_id(&bytes_from_id).unwrap();
		assert_eq!(primitive_from_id, deserialized_id);

		// Test ordering
		let virtual1 = ShapeId::vtable(1);
		let virtual2 = ShapeId::vtable(2);
		let bytes1 = serialize_shape_id(&virtual1);
		let bytes2 = serialize_shape_id(&virtual2);
		// In descending order, larger values should have smaller byte
		// representations
		assert!(bytes2 < bytes1, "vtable(2) should be < vtable(1) in bytes");
	}

	#[test]
	fn test_index_id_serialization() {
		// Test basic serialization/deserialization
		let index = IndexId::primary(42);
		let bytes = serialize_index_id(&index);
		let deserialized = deserialize_index_id(&bytes).unwrap();
		assert_eq!(index.as_u64(), deserialized.as_u64());

		// Test that type byte is 0x01 for Primary
		assert_eq!(bytes[0], 0x01);

		// Test with PrimaryKeyId directly
		let primary_id = PrimaryKeyId(123);
		let index_from_id = IndexId::Primary(primary_id);
		let bytes_from_id = serialize_index_id(&index_from_id);
		let deserialized_id = deserialize_index_id(&bytes_from_id).unwrap();
		assert_eq!(index_from_id.as_u64(), deserialized_id.as_u64());
	}

	#[test]
	fn test_index_id_ordering() {
		// Test that larger IDs encode to smaller byte sequences
		// (descending order)
		let index1 = IndexId::primary(1);
		let index2 = IndexId::primary(2);
		let index100 = IndexId::primary(100);
		let index200 = IndexId::primary(200);

		let bytes1 = serialize_index_id(&index1);
		let bytes2 = serialize_index_id(&index2);
		let bytes100 = serialize_index_id(&index100);
		let bytes200 = serialize_index_id(&index200);

		// In descending order, larger values should have smaller byte
		// representations
		assert!(bytes2 < bytes1, "index(2) should be < index(1) in bytes");
		assert!(bytes200 < bytes100, "index(200) should be < index(100) in bytes");
		assert!(bytes100 < bytes2, "index(100) should be < index(2) in bytes");
	}

	#[test]
	fn test_index_id_range_boundaries() {
		// Test range boundary creation for indexes
		let index10 = IndexId::primary(10);
		let index11 = IndexId::primary(11);

		let bytes10 = serialize_index_id(&index10);
		let bytes11 = serialize_index_id(&index11);

		// In descending order, index(11) < index(10)
		assert!(bytes11 < bytes10, "index(11) should be < index(10) in bytes");

		// Verify the structure: [type_byte, ...id_bytes]
		assert_eq!(bytes10.len(), 9, "IndexId should be 9 bytes");
		assert_eq!(bytes10[0], 0x01, "Primary variant should have type byte 0x01");

		// Test that incrementing the ID works for range end bounds
		let next_index = IndexId::primary(11);
		let next_bytes = serialize_index_id(&next_index);

		// In descending order, the next index should be less than the
		// current
		assert!(next_bytes < bytes10, "index(11) should be < index(10) for proper range boundaries");
	}

	#[test]
	fn test_index_entry_key_encoding_with_discriminator() {
		// Simulate IndexEntryKey encoding with proper discriminators
		let shape = ShapeId::table(42);
		let index = IndexId::primary(7);

		let primitive_bytes = serialize_shape_id(&shape);
		let index_bytes = serialize_index_id(&index);

		// Verify sizes
		assert_eq!(primitive_bytes.len(), 9, "ShapeId should be 9 bytes");
		assert_eq!(index_bytes.len(), 9, "IndexId should be 9 bytes");

		// Verify discriminators
		assert_eq!(primitive_bytes[0], 0x01, "Table shape should have type byte 0x01");
		assert_eq!(index_bytes[0], 0x01, "Primary index should have type byte 0x01");

		// Total key prefix would be: version(1) + kind(1) + shape(9) +
		// index(9) = 20 bytes
		let total_prefix_size = 1 + 1 + primitive_bytes.len() + index_bytes.len();
		assert_eq!(total_prefix_size, 20, "Total IndexEntryKey prefix should be 20 bytes");
	}
}
