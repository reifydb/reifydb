// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::return_internal_error;

use crate::interface::{IndexId, PrimaryKeyId, RingBufferId, SourceId, TableId, TableVirtualId, ViewId};

/// Serialize a SourceId for use in database keys
/// Returns [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View, 0x03 for TableVirtual, 0x04 for RingBuffer
pub fn serialize_source_id(source: &SourceId) -> Vec<u8> {
	let mut result = Vec::with_capacity(9);
	match source {
		SourceId::Table(TableId(id)) => {
			result.push(0x01);
			result.extend(&super::serialize(id));
		}
		SourceId::View(ViewId(id)) => {
			result.push(0x02);
			result.extend(&super::serialize(id));
		}
		SourceId::TableVirtual(TableVirtualId(id)) => {
			result.push(0x03);
			result.extend(&super::serialize(id));
		}
		SourceId::RingBuffer(RingBufferId(id)) => {
			result.push(0x04);
			result.extend(&super::serialize(id));
		}
	}
	result
}

/// Deserialize a SourceId from database key bytes
/// Expects [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View, 0x03 for TableVirtual, 0x04 for RingBuffer
pub fn deserialize_source_id(bytes: &[u8]) -> crate::Result<SourceId> {
	if bytes.len() != 9 {
		return_internal_error!("Invalid SourceId encoding: expected 9 bytes, got {}", bytes.len());
	}

	let type_byte = bytes[0];
	let id: u64 = super::deserialize(&bytes[1..9])?;

	match type_byte {
		0x01 => Ok(SourceId::Table(TableId(id))),
		0x02 => Ok(SourceId::View(ViewId(id))),
		0x03 => Ok(SourceId::TableVirtual(TableVirtualId(id))),
		0x04 => Ok(SourceId::RingBuffer(RingBufferId(id))),
		_ => return_internal_error!("Invalid SourceId type byte: 0x{:02x}.", type_byte),
	}
}

/// Serialize an IndexId for use in database keys
/// Returns [type_byte, ...id_bytes]
pub fn serialize_index_id(index: &IndexId) -> Vec<u8> {
	let mut result = Vec::with_capacity(9);
	match index {
		IndexId::Primary(PrimaryKeyId(id)) => {
			result.push(0x01);
			result.extend(&super::serialize(id));
		} // Future: Secondary, Unique, etc.
	}
	result
}

/// Deserialize an IndexId from database key bytes
/// Expects [type_byte, ...id_bytes]
pub fn deserialize_index_id(bytes: &[u8]) -> crate::Result<IndexId> {
	if bytes.len() != 9 {
		return_internal_error!("Invalid IndexId encoding: expected 9 bytes, got {}", bytes.len());
	}

	let type_byte = bytes[0];
	let id: u64 = super::deserialize(&bytes[1..9])?;

	match type_byte {
		0x01 => Ok(IndexId::Primary(PrimaryKeyId(id))),
		// Future: 0x02 => Ok(IndexId::Secondary(...)), etc.
		_ => return_internal_error!("Invalid IndexId type byte: 0x{:02x}.", type_byte),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_source_id_ordering() {
		// Test that larger IDs encode to smaller byte sequences
		// (descending order)
		let source1 = SourceId::table(1);
		let source2 = SourceId::table(2);
		let source100 = SourceId::table(100);
		let source200 = SourceId::table(200);

		let bytes1 = serialize_source_id(&source1);
		let bytes2 = serialize_source_id(&source2);
		let bytes100 = serialize_source_id(&source100);
		let bytes200 = serialize_source_id(&source200);

		// In descending order, larger values should have smaller byte
		// representations
		assert!(bytes2 < bytes1, "source(2) should be < source(1) in bytes");
		assert!(bytes200 < bytes100, "source(200) should be < source(100) in bytes");
		assert!(bytes100 < bytes2, "source(100) should be < source(2) in bytes");
	}

	#[test]
	fn test_range_boundaries() {
		// Test range boundary creation for tables
		let source10 = SourceId::table(10);
		let source9 = source10.prev();

		let bytes10 = serialize_source_id(&source10);
		let bytes9 = serialize_source_id(&source9);

		// In descending order, source(9) > source(10)
		assert!(bytes9 > bytes10, "source(9) should be > source(10) in bytes");

		// Test with views
		let view10 = SourceId::view(10);
		let view9 = view10.prev();

		let vbytes10 = serialize_source_id(&view10);
		let vbytes9 = serialize_source_id(&view9);

		// In descending order, view(9) > view(10)
		assert!(vbytes9 > vbytes10, "view(9) should be > view(10) in bytes");

		// Test with virtual tables
		let virtual10 = SourceId::table_virtual(10);
		let virtual9 = virtual10.prev();

		let tvbytes10 = serialize_source_id(&virtual10);
		let tvbytes9 = serialize_source_id(&virtual9);

		// In descending order, table_virtual(9) > table_virtual(10)
		assert!(tvbytes9 > tvbytes10, "table_virtual(9) should be > table_virtual(10) in bytes");

		// Check that view, table, and table_virtual with same ID encode
		// differently
		assert_ne!(bytes10, vbytes10, "table(10) should != view(10)");
		assert_ne!(bytes10, tvbytes10, "table(10) should != table_virtual(10)");
		assert_ne!(vbytes10, tvbytes10, "view(10) should != table_virtual(10)");
		assert_eq!(bytes10[0], 0x01, "table type byte should be 0x01");
		assert_eq!(vbytes10[0], 0x02, "view type byte should be 0x02");
		assert_eq!(tvbytes10[0], 0x03, "table_virtual type byte should be 0x03");

		// Simulate what happens with encoded keys
		let row_key_10_100 = vec![0xFE, 0xFC]; // version, kind
		let mut key1 = row_key_10_100.clone();
		key1.extend(&bytes10);
		key1.extend(&super::super::serialize(&100u64)); // encoded 100

		let mut key2 = row_key_10_100.clone();
		key2.extend(&bytes10);
		key2.extend(&super::super::serialize(&200u64)); // encoded 200

		let mut end_key = vec![0xFE, 0xFC];
		end_key.extend(&bytes9);

		// Range check assertions
		assert!(key1 >= bytes10, "key1 should be >= start(source10)");
		assert!(key1 < end_key, "key1 should be < end(source9)");
		assert!(key2 >= bytes10, "key2 should be >= start(source10)");
		assert!(key2 < end_key, "key2 should be < end(source9)");
	}

	#[test]
	fn test_table_virtual_serialization() {
		use crate::interface::TableVirtualId;

		// Test basic serialization/deserialization
		let virtual_source = SourceId::table_virtual(42);
		let bytes = serialize_source_id(&virtual_source);
		let deserialized = deserialize_source_id(&bytes).unwrap();
		assert_eq!(virtual_source, deserialized);

		// Test that type byte is 0x03
		assert_eq!(bytes[0], 0x03);

		// Test with TableVirtualId directly
		let virtual_id = TableVirtualId(123);
		let source_from_id = SourceId::from(virtual_id);
		let bytes_from_id = serialize_source_id(&source_from_id);
		let deserialized_id = deserialize_source_id(&bytes_from_id).unwrap();
		assert_eq!(source_from_id, deserialized_id);

		// Test ordering
		let virtual1 = SourceId::table_virtual(1);
		let virtual2 = SourceId::table_virtual(2);
		let bytes1 = serialize_source_id(&virtual1);
		let bytes2 = serialize_source_id(&virtual2);
		// In descending order, larger values should have smaller byte
		// representations
		assert!(bytes2 < bytes1, "table_virtual(2) should be < table_virtual(1) in bytes");
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
		let source = SourceId::table(42);
		let index = IndexId::primary(7);

		let source_bytes = serialize_source_id(&source);
		let index_bytes = serialize_index_id(&index);

		// Verify sizes
		assert_eq!(source_bytes.len(), 9, "SourceId should be 9 bytes");
		assert_eq!(index_bytes.len(), 9, "IndexId should be 9 bytes");

		// Verify discriminators
		assert_eq!(source_bytes[0], 0x01, "Table source should have type byte 0x01");
		assert_eq!(index_bytes[0], 0x01, "Primary index should have type byte 0x01");

		// Total key prefix would be: version(1) + kind(1) + source(9) +
		// index(9) = 20 bytes
		let total_prefix_size = 1 + 1 + source_bytes.len() + index_bytes.len();
		assert_eq!(total_prefix_size, 20, "Total IndexEntryKey prefix should be 20 bytes");
	}
}
