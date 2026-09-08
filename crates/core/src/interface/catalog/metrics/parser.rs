// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::deserializer::KeyDeserializer;
use reifydb_value::value::dictionary::DictionaryId;

use crate::{
	interface::catalog::{metrics::MetricsId, object::ObjectId},
	key::{catalog::KeyDeserializerCatalogExt, tag::KeyTag},
};

pub fn parse_id(key: &[u8]) -> MetricsId {
	let Some(kind) = KeyTag::of(key) else {
		return MetricsId::System;
	};
	extract_metrics_id(key, kind)
}

fn extract_metrics_id(key: &[u8], kind: KeyTag) -> MetricsId {
	match kind {
		KeyTag::Row
		| KeyTag::SeriesRow
		| KeyTag::PartitionedRow
		| KeyTag::PartitionedSeriesRow
		| KeyTag::SortedViewRow
		| KeyTag::PartitionedSortedViewRow
		| KeyTag::RowSequence
		| KeyTag::Column
		| KeyTag::Columns
		| KeyTag::ColumnSequence
		| KeyTag::ColumnProperty
		| KeyTag::Index
		| KeyTag::IndexEntry
		| KeyTag::PrimaryKey => extract_object_id(key).map(MetricsId::Object).unwrap_or(MetricsId::System),

		KeyTag::DictionaryEntry | KeyTag::DictionaryEntryIndex => extract_dictionary_id(key)
			.map(|id| MetricsId::Object(ObjectId::Dictionary(DictionaryId(id))))
			.unwrap_or(MetricsId::System),

		_ => MetricsId::System,
	}
}

fn extract_object_id(key: &[u8]) -> Option<ObjectId> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	de.read_object_id().ok()
}

fn extract_dictionary_id(key: &[u8]) -> Option<u64> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = de.read_u8().ok()?;
	de.read_u64().ok()
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{dictionary::DictionaryId, row_number::RowNumber};

	use super::*;
	use crate::{
		interface::catalog::{object::ObjectId, storage::StorageId},
		key::{catalog::DictionaryEntryKey, row::RowKey},
	};

	#[test]
	fn test_parse_object_id_row() {
		let object = ObjectId::table(42);
		let encoded = RowKey::encoded(StorageId::table(42), RowNumber(100));

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricsId::Object(object));
	}

	#[test]
	fn test_parse_object_id_system() {
		let fake_key = vec![0xFE, 0x01, 0, 0, 0, 0];
		let id = parse_id(&fake_key);
		assert_eq!(id, MetricsId::System);
	}

	#[test]
	fn test_parse_object_id_dictionary() {
		let dictionary_id = DictionaryId(789);
		let hash = [0u8; 16];
		let key = DictionaryEntryKey::new(dictionary_id, hash);
		let encoded = key.encode();

		let id = parse_id(encoded.as_slice());
		assert_eq!(id, MetricsId::Object(ObjectId::Dictionary(dictionary_id)));
	}
}
