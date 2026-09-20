// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column_snapshot::ColumnStats;
use reifydb_macro::catalog_shape;
use reifydb_value::value::{Value, blob::Blob};

catalog_shape! {
	pub(crate) column_snapshot {
		id: u64,
		namespace: u64,
		kind: u8,
		source_id: u64,
		bucket_start: u64,
		bucket_width: u64,
		partition_hi: u64?,
		partition_lo: u64?,
		sequence_counter: u64,
		read_version: u64,
		row_count: u64,
		partition_values: blob,
		stats: blob,
	}
}

const STATS_ENCODING_V1: u8 = 1;

pub(crate) fn serialize_partition_values(values: &[Value]) -> Blob {
	Blob::from(postcard::to_allocvec(values).expect("postcard serialization of a Value is total"))
}

pub(crate) fn deserialize_partition_values(blob: &Blob) -> Vec<Value> {
	let bytes = blob.as_bytes();
	if bytes.is_empty() {
		return Vec::new();
	}
	postcard::from_bytes(bytes).expect("stored column snapshot partition values must decode")
}

pub(crate) fn serialize_stats(stats: &[ColumnStats]) -> Blob {
	let mut bytes = vec![STATS_ENCODING_V1];
	bytes = postcard::to_extend(stats, bytes).expect("postcard serialization of ColumnStats is total");
	Blob::from(bytes)
}

pub(crate) fn deserialize_stats(blob: &Blob) -> Vec<ColumnStats> {
	let Some((version, rest)) = blob.as_bytes().split_first() else {
		return Vec::new();
	};
	assert_eq!(*version, STATS_ENCODING_V1, "unknown column snapshot stats encoding version {version}");
	postcard::from_bytes(rest).expect("stored column snapshot stats must decode")
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::column_snapshot::ColumnStats;
	use reifydb_value::value::Value;

	use super::{
		deserialize_partition_values, deserialize_stats, serialize_partition_values, serialize_stats,
	};

	#[test]
	fn test_partition_values_round_trip_preserves_value_types() {
		// A joined string encoding would collapse these three onto the same bytes;
		// the round trip must return each value with its original type.
		let original = vec![Value::Utf8("us".to_string()), Value::Uint8(1), Value::Int4(-1)];
		let decoded = deserialize_partition_values(&serialize_partition_values(&original));
		assert_eq!(decoded, original);
	}

	#[test]
	fn test_empty_partition_values_round_trip() {
		let decoded = deserialize_partition_values(&serialize_partition_values(&[]));
		assert!(decoded.is_empty());
	}

	#[test]
	fn test_stats_round_trip_preserves_none_min_max() {
		// An all-none column has no min and no max; collapsing those onto a zero
		// would let pruning drop a block that still holds matching rows.
		let original = vec![
			ColumnStats {
				column: "value".to_string(),
				min: Some(Value::Int4(-5)),
				max: Some(Value::Int4(9)),
				none_count: 2,
			},
			ColumnStats {
				column: "label".to_string(),
				min: None,
				max: None,
				none_count: 7,
			},
		];
		let decoded = deserialize_stats(&serialize_stats(&original));
		assert_eq!(decoded, original);
	}

	#[test]
	fn test_stats_blob_carries_its_version_byte() {
		// The version prefix is what lets a later encoding be told apart from this one.
		let blob = serialize_stats(&[]);
		assert_eq!(blob.as_bytes().first(), Some(&1u8));
	}

	#[test]
	#[should_panic(expected = "unknown column snapshot stats encoding version")]
	fn test_stats_with_unknown_version_panics() {
		// An unrecognised version must stop the read, never decode as version 1.
		deserialize_stats(&reifydb_value::value::blob::Blob::from(vec![2u8, 0u8]));
	}
}
