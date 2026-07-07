// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

use super::{EncodableKey, KeyKind};
use crate::{
	interface::catalog::shape::ShapeId,
	key::catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
};

#[derive(Debug, Clone, PartialEq)]
pub enum RowLocator {
	Row(RowNumber),

	Series {
		variant_tag: Option<u8>,
		key: u64,
		sequence: u64,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedRowKey {
	pub shape: ShapeId,
	pub partition: Partition,
	pub locator: RowLocator,
}

impl PartitionedRowKey {
	pub fn new(shape: impl Into<ShapeId>, partition: Partition, locator: RowLocator) -> Self {
		Self {
			shape: shape.into(),
			partition,
			locator,
		}
	}

	pub fn encoded(shape: impl Into<ShapeId>, partition: Partition, locator: RowLocator) -> EncodedKey {
		Self::new(shape, partition, locator).encode()
	}

	pub fn shape_of(key: &EncodedKey) -> Option<ShapeId> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}
		de.read_shape_id().ok()
	}

	pub fn full_scan(shape: impl Into<ShapeId>) -> EncodedKeyRange {
		let shape = shape.into();
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(Self::KIND as u8).extend_shape_id(shape);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_shape_id(shape.prev());
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
	}

	pub fn scan_range(shape: impl Into<ShapeId>, last_key: Option<&EncodedKey>) -> EncodedKeyRange {
		let shape = shape.into();
		let start = match last_key {
			Some(last) => Bound::Excluded(last.clone()),
			None => {
				let mut start = KeySerializer::with_capacity(10);
				start.extend_u8(Self::KIND as u8).extend_shape_id(shape);
				Bound::Included(start.to_encoded_key())
			}
		};
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_shape_id(shape.prev());
		EncodedKeyRange::new(start, Bound::Included(end.to_encoded_key()))
	}

	pub fn partition_range(shape: impl Into<ShapeId>, partition: Partition) -> EncodedKeyRange {
		let shape = shape.into();
		let mut prefix = KeySerializer::with_capacity(26);
		prefix.extend_u8(Self::KIND as u8).extend_shape_id(shape).extend_u128(partition.0);
		let start = prefix.to_encoded_key();
		let end = prefix_successor(start.as_slice());
		EncodedKeyRange::new(Bound::Included(start), end)
	}

	pub fn partition_scan_range(
		shape: impl Into<ShapeId>,
		partition: Partition,
		last_key: Option<&EncodedKey>,
	) -> EncodedKeyRange {
		let base = Self::partition_range(shape, partition);
		match last_key {
			Some(last) => EncodedKeyRange::new(Bound::Excluded(last.clone()), base.end),
			None => base,
		}
	}
}

fn prefix_successor(prefix: &[u8]) -> Bound<EncodedKey> {
	let mut end = prefix.to_vec();
	while let Some(&last) = end.last() {
		if last == 0xFF {
			end.pop();
		} else {
			*end.last_mut().unwrap() = last + 1;
			return Bound::Excluded(EncodedKey::new(end));
		}
	}
	Bound::Unbounded
}

impl EncodableKey for PartitionedRowKey {
	const KIND: KeyKind = KeyKind::PartitionedRow;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_u8(Self::KIND as u8).extend_shape_id(self.shape).extend_u128(self.partition.0);
		match &self.locator {
			RowLocator::Row(row) => {
				serializer.extend_u64(row.0);
			}
			RowLocator::Series {
				variant_tag,
				key,
				sequence,
			} => {
				match variant_tag {
					Some(tag) => {
						serializer.extend_u8(1u8).extend_u8(*tag);
					}
					None => {
						serializer.extend_u8(0u8);
					}
				}
				serializer.extend_u64(*key).extend_u64(*sequence);
			}
		}
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != Self::KIND {
			return None;
		}

		let shape = de.read_shape_id().ok()?;
		let partition = Partition(de.read_u128().ok()?);

		let locator = match shape {
			ShapeId::Series(_) => {
				let has_tag = de.read_u8().ok()?;
				let variant_tag = if has_tag == 1 {
					Some(de.read_u8().ok()?)
				} else {
					None
				};
				let key = de.read_u64().ok()?;
				let sequence = de.read_u64().ok()?;
				RowLocator::Series {
					variant_tag,
					key,
					sequence,
				}
			}
			_ => RowLocator::Row(RowNumber(de.read_u64().ok()?)),
		};

		Some(Self {
			shape,
			partition,
			locator,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::{EncodableKey, PartitionedRowKey, RowLocator};
	use crate::interface::catalog::{
		id::{SeriesId, TableId},
		shape::ShapeId,
	};

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	#[test]
	fn test_table_roundtrip() {
		let key = PartitionedRowKey {
			shape: ShapeId::Table(TableId(7)),
			partition: part("us"),
			locator: RowLocator::Row(RowNumber(42)),
		};
		let decoded = PartitionedRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_series_roundtrip_with_tag() {
		let key = PartitionedRowKey {
			shape: ShapeId::Series(SeriesId(3)),
			partition: part("btc"),
			locator: RowLocator::Series {
				variant_tag: Some(5),
				key: 1_700_000_000,
				sequence: 9,
			},
		};
		let decoded = PartitionedRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_series_roundtrip_without_tag() {
		let key = PartitionedRowKey {
			shape: ShapeId::Series(SeriesId(3)),
			partition: part("eth"),
			locator: RowLocator::Series {
				variant_tag: None,
				key: 100,
				sequence: 0,
			},
		};
		let decoded = PartitionedRowKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_shape_of() {
		let key = PartitionedRowKey::encoded(
			ShapeId::Table(TableId(42)),
			part("us"),
			RowLocator::Row(RowNumber(1)),
		);
		assert_eq!(PartitionedRowKey::shape_of(&key), Some(ShapeId::Table(TableId(42))));
	}

	#[test]
	fn test_partition_rows_cluster_together() {
		let shape = ShapeId::Table(TableId(1));
		let us_a = PartitionedRowKey::encoded(shape, part("us"), RowLocator::Row(RowNumber(1)));
		let us_b = PartitionedRowKey::encoded(shape, part("us"), RowLocator::Row(RowNumber(2)));
		let eu = PartitionedRowKey::encoded(shape, part("eu"), RowLocator::Row(RowNumber(1)));

		let mut keys = [us_a.clone(), us_b.clone(), eu.clone()];
		keys.sort();
		let us_positions: Vec<usize> =
			keys.iter().enumerate().filter(|(_, k)| **k == us_a || **k == us_b).map(|(i, _)| i).collect();
		assert_eq!(us_positions[1] - us_positions[0], 1, "us partition rows must be contiguous");
	}

	#[test]
	fn test_partition_range_contains_only_its_partition() {
		let shape = ShapeId::Table(TableId(1));
		let range = PartitionedRowKey::partition_range(shape, part("us"));
		let us = PartitionedRowKey::encoded(shape, part("us"), RowLocator::Row(RowNumber(500)));
		let eu = PartitionedRowKey::encoded(shape, part("eu"), RowLocator::Row(RowNumber(1)));
		assert!(range.contains(&us), "us row must be inside the us partition range");
		assert!(!range.contains(&eu), "eu row must be outside the us partition range");
	}
}
