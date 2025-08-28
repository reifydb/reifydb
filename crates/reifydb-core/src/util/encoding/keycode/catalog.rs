// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	interface::{StoreId, TableId, ViewId},
	return_internal_error,
};

/// Serialize a StoreId for use in database keys
/// Returns [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View
pub fn serialize_store_id(store: &StoreId) -> Vec<u8> {
	let mut result = Vec::with_capacity(9);
	match store {
		StoreId::Table(TableId(id)) => {
			result.push(0x01);
			result.extend(&super::serialize(id));
		}
		StoreId::View(ViewId(id)) => {
			result.push(0x02);
			result.extend(&super::serialize(id));
		}
	}
	result
}

/// Deserialize a StoreId from database key bytes
/// Expects [type_byte, ...id_bytes] where type_byte is 0x01 for Table, 0x02 for
/// View
pub fn deserialize_store_id(bytes: &[u8]) -> crate::Result<StoreId> {
	if bytes.len() != 9 {
		return_internal_error!(
			"Invalid StoreId encoding: expected 9 bytes, got {}",
			bytes.len()
		);
	}

	let type_byte = bytes[0];
	let id: u64 = super::deserialize(&bytes[1..9])?;

	match type_byte {
		0x01 => Ok(StoreId::Table(TableId(id))),
		0x02 => Ok(StoreId::View(ViewId(id))),
		_ => return_internal_error!(
			"Invalid StoreId type byte: 0x{:02x}. Expected 0x01 (Table) or 0x02 (View)",
			type_byte
		),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_store_id_ordering() {
		// Test that larger IDs encode to smaller byte sequences
		// (descending order)
		let store1 = StoreId::table(1);
		let store2 = StoreId::table(2);
		let store100 = StoreId::table(100);
		let store200 = StoreId::table(200);

		let bytes1 = serialize_store_id(&store1);
		let bytes2 = serialize_store_id(&store2);
		let bytes100 = serialize_store_id(&store100);
		let bytes200 = serialize_store_id(&store200);

		println!("store(1) = {:02x?}", bytes1);
		println!("store(2) = {:02x?}", bytes2);
		println!("store(100) = {:02x?}", bytes100);
		println!("store(200) = {:02x?}", bytes200);

		// In descending order, larger values should have smaller byte
		// representations
		assert!(
			bytes2 < bytes1,
			"store(2) should be < store(1) in bytes"
		);
		assert!(
			bytes200 < bytes100,
			"store(200) should be < store(100) in bytes"
		);
		assert!(
			bytes100 < bytes2,
			"store(100) should be < store(2) in bytes"
		);
	}

	#[test]
	fn test_range_boundaries() {
		// Test range boundary creation for tables
		let store10 = StoreId::table(10);
		let store9 = store10.prev();

		let bytes10 = serialize_store_id(&store10);
		let bytes9 = serialize_store_id(&store9);

		println!("Table test:");
		println!("store(10) = {:02x?}", bytes10);
		println!("store(9) = {:02x?}", bytes9);
		println!(
			"In descending order, store(9) > store(10): {}",
			bytes9 > bytes10
		);

		// Test with views
		let view10 = StoreId::view(10);
		let view9 = view10.prev();

		let vbytes10 = serialize_store_id(&view10);
		let vbytes9 = serialize_store_id(&view9);

		println!("\nView test:");
		println!("view(10) = {:02x?}", vbytes10);
		println!("view(9) = {:02x?}", vbytes9);
		println!(
			"In descending order, view(9) > view(10): {}",
			vbytes9 > vbytes10
		);

		// Check that view and table with same ID encode differently
		println!("\nTable vs View:");
		println!("table(10) != view(10): {}", bytes10 != vbytes10);
		println!("table type byte: 0x01, view type byte: 0x02");

		// Simulate what happens with row keys
		let row_key_10_100 = vec![0xFE, 0xFC]; // version, kind
		let mut key1 = row_key_10_100.clone();
		key1.extend(&bytes10);
		key1.extend(&super::super::serialize(&100u64)); // row 100

		let mut key2 = row_key_10_100.clone();
		key2.extend(&bytes10);
		key2.extend(&super::super::serialize(&200u64)); // row 200

		let mut end_key = vec![0xFE, 0xFC];
		end_key.extend(&bytes9);

		println!("\nTable row keys:");
		println!("key(store10, row100) = {:02x?}", key1);
		println!("key(store10, row200) = {:02x?}", key2);
		println!("end_key(store9) = {:02x?}", end_key);

		println!("\nRange check:");
		println!("  key1 >= start(store10): {}", key1 >= bytes10);
		println!("  key1 < end(store9): {}", key1 < end_key);
		println!("  key2 >= start(store10): {}", key2 >= bytes10);
		println!("  key2 < end(store9): {}", key2 < end_key);
	}
}
