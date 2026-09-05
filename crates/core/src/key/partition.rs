// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_macro::Key;
use reifydb_value::value::partition::Partition;

use super::KeyKind;
use crate::{
	interface::catalog::object::ObjectId,
	key::{
		any::{Field, KeyFields, Width},
		bound::{AnyKeyBound, AnyKeyBoundRange, object_fields},
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::key::Key,
	},
};

#[derive(Debug, Clone, PartialEq, Key, Hash)]
#[key(kind = Partition)]
pub struct PartitionKey {
	pub object: ObjectId,
	pub partition: Partition,
}

impl PartitionKey {
	pub fn new(object: impl Into<ObjectId>, partition: Partition) -> Self {
		Self {
			object: object.into(),
			partition,
		}
	}

	pub fn encoded(object: impl Into<ObjectId>, partition: Partition) -> EncodedKey {
		Self::new(object, partition).encode()
	}

	pub fn full_scan(object: impl Into<ObjectId>) -> AnyKeyBoundRange {
		let object = object.into();
		AnyKeyBoundRange::start_end(
			AnyKeyBound::prefix(Self::KIND, object_fields(object)),
			AnyKeyBound::prefix(Self::KIND, object_fields(object.prev())),
		)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::RangeBounds;

	use reifydb_value::value::{Value, partition::Partition};

	use super::PartitionKey;
	use crate::{
		interface::catalog::{id::TableId, object::ObjectId},
		key::typed::key::Key,
	};

	#[test]
	fn test_roundtrip() {
		let key = PartitionKey {
			object: ObjectId::Table(TableId(7)),
			partition: Partition::of(&[Value::Utf8("us".to_string())]),
		};
		let decoded = PartitionKey::decode(&key.encode()).unwrap();
		assert_eq!(decoded, key);
	}

	#[test]
	fn test_partitions_of_object_share_prefix() {
		let object = ObjectId::Table(TableId(3));
		let range = PartitionKey::full_scan(object).encode();
		let k = PartitionKey::encoded(object, Partition::of(&[Value::Utf8("us".to_string())]));
		assert!(range.contains(&k));
		let other = PartitionKey::encoded(
			ObjectId::Table(TableId(4)),
			Partition::of(&[Value::Utf8("us".to_string())]),
		);
		assert!(!range.contains(&other));
	}
}
