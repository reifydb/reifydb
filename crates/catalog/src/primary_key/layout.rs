// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod primary_key {
	use once_cell::sync::Lazy;
	use reifydb_core::{interface::ColumnId, value::row::EncodedRowLayout};
	use reifydb_type::{Blob, Type};

	pub(crate) const ID: usize = 0;
	pub(crate) const SOURCE: usize = 1;
	pub(crate) const COLUMN_IDS: usize = 2;

	pub(crate) static LAYOSVT: Lazy<EncodedRowLayout> = Lazy::new(|| {
		EncodedRowLayout::new(&[
			Type::Uint8, // id - Primary key ID
			Type::Uint8, // source
			Type::Blob,  // column_ids
		])
	});

	/// Serialize a list of column IDs into a blob
	/// Format: 8 bytes for count, followed by 8 bytes per column ID
	pub(crate) fn serialize_column_ids(column_ids: &[ColumnId]) -> Blob {
		let mut bytes = Vec::new();

		// Write count
		bytes.extend_from_slice(&(column_ids.len() as u64).to_le_bytes());

		// Write each column ID
		for col_id in column_ids {
			bytes.extend_from_slice(&col_id.0.to_le_bytes());
		}

		Blob::from(bytes)
	}

	/// Deserialize a blob into a list of column IDs
	/// Format: 8 bytes for count, followed by 8 bytes per column ID
	pub(crate) fn deserialize_column_ids(blob: &Blob) -> Vec<ColumnId> {
		let bytes = blob.as_ref();

		// Read count
		let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;

		// Read each column ID
		let mut column_ids = Vec::with_capacity(count);
		for i in 0..count {
			let start = 8 + i * 8;
			let id = u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
			column_ids.push(ColumnId(id));
		}

		column_ids
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::ColumnId;

	use super::primary_key::{deserialize_column_ids, serialize_column_ids};

	#[test]
	fn test_serialize_deserialize_column_ids() {
		let test_cases = vec![
			// Empty list
			vec![],
			// Single column
			vec![ColumnId(1)],
			// Multiple columns
			vec![ColumnId(1), ColumnId(2), ColumnId(3)],
			// Large IDs
			vec![ColumnId(u64::MAX), ColumnId(u64::MAX - 1)],
			// Many columns
			vec![
				ColumnId(10),
				ColumnId(20),
				ColumnId(30),
				ColumnId(40),
				ColumnId(50),
				ColumnId(60),
				ColumnId(70),
				ColumnId(80),
				ColumnId(90),
				ColumnId(100),
			],
			// Sequential IDs
			(0..20).map(ColumnId).collect::<Vec<_>>(),
			// Non-sequential IDs
			vec![ColumnId(100), ColumnId(1), ColumnId(50), ColumnId(25)],
		];

		for original in test_cases {
			let blob = serialize_column_ids(&original);
			let deserialized = deserialize_column_ids(&blob);

			assert_eq!(original, deserialized, "Failed to round-trip column IDs: {:?}", original);

			// Verify blob format: first 8 bytes should be the count
			let bytes = blob.as_ref();
			if !original.is_empty() || bytes.len() >= 8 {
				let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
				assert_eq!(
					count as usize,
					original.len(),
					"Serialized count mismatch for {:?}",
					original
				);

				// Verify total size
				assert_eq!(
					bytes.len(),
					8 + original.len() * 8,
					"Serialized size mismatch for {:?}",
					original
				);
			}
		}
	}

	#[test]
	fn test_serialize_format() {
		// Test specific format details
		let column_ids = vec![ColumnId(0x0123456789ABCDEF), ColumnId(0xFEDCBA9876543210)];
		let blob = serialize_column_ids(&column_ids);
		let bytes = blob.as_ref();

		// Check count (2 in little-endian)
		assert_eq!(&bytes[0..8], &[2, 0, 0, 0, 0, 0, 0, 0]);

		// Check first ID (0x0123456789ABCDEF in little-endian)
		assert_eq!(&bytes[8..16], &[0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]);

		// Check second ID (0xFEDCBA9876543210 in little-endian)
		assert_eq!(&bytes[16..24], &[0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE]);
	}
}
