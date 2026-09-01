// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_value::value::partition::Partition;

use super::KeyKind;
use crate::{
	interface::catalog::object::ObjectId,
	key::{
		catalog::{KeyDeserializerCatalogExt, KeySerializerCatalogExt},
		typed::key::Key,
	},
};

#[derive(Debug, Clone, PartialEq, Key)]
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

	pub fn full_scan(object: impl Into<ObjectId>) -> EncodedKeyRange {
		let object = object.into();
		let mut start = KeySerializer::with_capacity(10);
		start.extend_u8(Self::KIND as u8).extend_object_id(object);
		let mut end = KeySerializer::with_capacity(10);
		end.extend_u8(Self::KIND as u8).extend_object_id(object.prev());
		EncodedKeyRange::start_end(Some(start.to_encoded_key()), Some(end.to_encoded_key()))
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
		let range = PartitionKey::full_scan(object);
		let k = PartitionKey::encoded(object, Partition::of(&[Value::Utf8("us".to_string())]));
		assert!(range.contains(&k));
		let other = PartitionKey::encoded(
			ObjectId::Table(TableId(4)),
			Partition::of(&[Value::Utf8("us".to_string())]),
		);
		assert!(!range.contains(&other));
	}
}
