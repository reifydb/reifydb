// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::ColumnId;
use reifydb_macro::catalog_shape;
use reifydb_value::value::blob::Blob;

catalog_shape! {
	pub(crate) primary_key {
		id: u64,
		source: u64,
		column_ids: blob,
	}
}

pub(crate) fn serialize_column_ids(column_ids: &[ColumnId]) -> Blob {
	let mut bytes = Vec::new();

	bytes.extend_from_slice(&(column_ids.len() as u64).to_le_bytes());

	for col_id in column_ids {
		bytes.extend_from_slice(&col_id.0.to_le_bytes());
	}

	Blob::from(bytes)
}

pub(crate) fn deserialize_column_ids(blob: &Blob) -> Vec<ColumnId> {
	let bytes = blob.as_bytes();

	let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;

	let mut column_ids = Vec::with_capacity(count);
	for i in 0..count {
		let start = 8 + i * 8;
		let id = u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
		column_ids.push(ColumnId(id));
	}

	column_ids
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ColumnId;

	use super::{deserialize_column_ids, serialize_column_ids};

	#[test]
	fn test_serialize_deserialize_column_ids() {
		let test_cases = vec![
			vec![],
			vec![ColumnId(1)],
			vec![ColumnId(1), ColumnId(2), ColumnId(3)],
			vec![ColumnId(u64::MAX), ColumnId(u64::MAX - 1)],
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
			(0..20).map(ColumnId).collect::<Vec<_>>(),
			vec![ColumnId(100), ColumnId(1), ColumnId(50), ColumnId(25)],
		];

		for original in test_cases {
			let blob = serialize_column_ids(&original);
			let deserialized = deserialize_column_ids(&blob);

			assert_eq!(original, deserialized, "Failed to round-trip column IDs: {:?}", original);

			let bytes = blob.as_bytes();
			if !original.is_empty() || bytes.len() >= 8 {
				let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
				assert_eq!(
					count as usize,
					original.len(),
					"Serialized count mismatch for {:?}",
					original
				);

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
		// The blob is a little-endian u64 count followed by the ids, also little-endian;
		// this is persisted layout, so the byte order is pinned here rather than round-tripped.
		let column_ids = vec![ColumnId(0x0123456789ABCDEF), ColumnId(0xFEDCBA9876543210)];
		let blob = serialize_column_ids(&column_ids);
		let bytes = blob.as_bytes();

		assert_eq!(&bytes[0..8], &[2, 0, 0, 0, 0, 0, 0, 0]);

		assert_eq!(&bytes[8..16], &[0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]);

		assert_eq!(&bytes[16..24], &[0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE]);
	}
}
