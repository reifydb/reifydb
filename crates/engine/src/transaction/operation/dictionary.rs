// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{dictionary::Dictionary, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
		store::SingleVersionGet,
	},
	internal_error,
	key::catalog::{DictionaryEntryIndexKey, DictionaryEntryKey},
	value::batch::single_row,
};
use reifydb_transaction::{
	dictionary::DictionaryAllocatorRegistry,
	interceptor::{WithInterceptors, dictionary_row::DictionaryRowInterceptor},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::{
	util::hash::xxh3_128,
	value::{Value, datetime::DateTime, dictionary::DictionaryEntryId},
};
use smallvec::smallvec;

use crate::Result;

pub(crate) trait DictionaryOperations {
	fn insert_into_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId> {
		let ids = self.intern_values(dictionary, vec![value.clone()])?;
		Ok(ids[0])
	}

	fn intern_values(&mut self, dictionary: &Dictionary, values: Vec<Value>) -> Result<Vec<DictionaryEntryId>>;

	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>>;

	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>>;
}

struct Interned {
	ids: Vec<DictionaryEntryId>,
	created: Vec<Value>,
}

fn intern_through_chains<T: WithInterceptors>(
	txn: &mut T,
	registry: impl FnOnce(&T) -> Option<DictionaryAllocatorRegistry>,
	dictionary: &Dictionary,
	mut values: Vec<Value>,
) -> Result<Interned> {
	if values.is_empty() {
		return Ok(Interned {
			ids: Vec::new(),
			created: Vec::new(),
		});
	}
	if !txn.dictionary_row_pre_insert_interceptors().is_empty() {
		DictionaryRowInterceptor::pre_insert(txn, dictionary, &mut values)?;
	}

	let registry =
		registry(txn).ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
	let outcomes = registry.intern_batch(dictionary, &values)?;

	let mut seen = HashSet::new();
	let mut created_ids = Vec::new();
	let mut created = Vec::new();
	for (outcome, value) in outcomes.iter().zip(values) {
		if outcome.created && seen.insert(outcome.id) {
			created_ids.push(outcome.id);
			created.push(value);
		}
	}
	if !created_ids.is_empty() && !txn.dictionary_row_post_insert_interceptors().is_empty() {
		DictionaryRowInterceptor::post_insert(txn, dictionary, &created_ids, &created)?;
	}

	Ok(Interned {
		ids: outcomes.into_iter().map(|outcome| outcome.id).collect(),
		created,
	})
}

impl DictionaryOperations for CommandTransaction {
	fn intern_values(&mut self, dictionary: &Dictionary, values: Vec<Value>) -> Result<Vec<DictionaryEntryId>> {
		Ok(intern_through_chains(self, CommandTransaction::dictionary_allocators, dictionary, values)?.ids)
	}

	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		let registry = self
			.dictionary_allocators()
			.ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
		match registry.get(dictionary, id.to_u128())? {
			Some(bytes) => {
				let value: Value = from_bytes(&bytes)
					.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?;
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		let registry = self
			.dictionary_allocators()
			.ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
		registry.find(dictionary, value)
	}
}

fn dictionary_insert_change(dictionary: &Dictionary, value: Value) -> Result<Change> {
	Ok(Change {
		origin: ChangeOrigin::Object(ObjectId::dictionary(dictionary.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::insert(single_row([("value", value)])?)],
		changed_at: DateTime::default(),
	})
}

impl DictionaryOperations for AdminTransaction {
	fn intern_values(&mut self, dictionary: &Dictionary, values: Vec<Value>) -> Result<Vec<DictionaryEntryId>> {
		Ok(intern_through_chains(self, AdminTransaction::dictionary_allocators, dictionary, values)?.ids)
	}

	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		let registry = self
			.dictionary_allocators()
			.ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
		match registry.get(dictionary, id.to_u128())? {
			Some(bytes) => {
				let value: Value = from_bytes(&bytes)
					.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?;
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		let registry = self
			.dictionary_allocators()
			.ok_or_else(|| internal_error!("dictionary allocator registry is not configured"))?;
		registry.find(dictionary, value)
	}
}

impl DictionaryOperations for Transaction<'_> {
	fn intern_values(&mut self, dictionary: &Dictionary, values: Vec<Value>) -> Result<Vec<DictionaryEntryId>> {
		match self {
			Transaction::Command(cmd) => cmd.intern_values(dictionary, values),
			Transaction::Admin(admin) => admin.intern_values(dictionary, values),
			Transaction::Test(t) => {
				let interned = intern_through_chains(
					t.inner,
					AdminTransaction::dictionary_allocators,
					dictionary,
					values,
				)?;
				for value in interned.created {
					t.inner.track_flow_change(dictionary_insert_change(dictionary, value)?);
				}
				Ok(interned.ids)
			}
			Transaction::Query(_) => {
				Err(internal_error!("Cannot insert into dictionary during a query transaction"))
			}
		}
	}

	fn get_from_dictionary(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		match self {
			Transaction::Command(cmd) => return cmd.get_from_dictionary(dictionary, id),
			Transaction::Admin(admin) => return admin.get_from_dictionary(dictionary, id),
			Transaction::Test(t) => return t.inner.get_from_dictionary(dictionary, id),
			Transaction::Query(_) => {}
		}

		let single = self
			.single()
			.ok_or_else(|| internal_error!("single-version store is not available for dictionary reads"))?;
		let store = single.read_store();
		let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, id.to_u128());
		match SingleVersionGet::get(&store, &index_key)? {
			Some(v) => {
				let value: Value = from_bytes(&v.bytes)
					.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?;
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn find_in_dictionary(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>> {
		match self {
			Transaction::Command(cmd) => return cmd.find_in_dictionary(dictionary, value),
			Transaction::Admin(admin) => return admin.find_in_dictionary(dictionary, value),
			Transaction::Test(t) => return t.inner.find_in_dictionary(dictionary, value),
			Transaction::Query(_) => {}
		}

		let value_bytes = to_stdvec(value).map_err(|e| internal_error!("Failed to serialize value: {}", e))?;
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		let single = self
			.single()
			.ok_or_else(|| internal_error!("single-version store is not available for dictionary reads"))?;
		let store = single.read_store();
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		match SingleVersionGet::get(&store, &entry_key)? {
			Some(v) => {
				if v.bytes.len() < 16 {
					return Err(internal_error!(
						"dictionary entry row is truncated: {} bytes",
						v.bytes.len()
					));
				}
				let mut id_bytes = [0u8; 16];
				id_bytes.copy_from_slice(&v.bytes[..16]);
				let entry_id = DictionaryEntryId::from_u128(
					u128::from_be_bytes(id_bytes),
					dictionary.id_type.clone(),
				)?;
				Ok(Some(entry_id))
			}
			None => Ok(None),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{dictionary::Dictionary, id::NamespaceId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::value::{
		Value,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::ValueType,
	};

	use super::DictionaryOperations;

	fn test_dictionary() -> Dictionary {
		Dictionary {
			id: DictionaryId(1),
			namespace: NamespaceId::SYSTEM,
			name: "test_dict".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint8,
		}
	}

	#[test]
	fn test_insert_into_dictionary() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		// Ids start at 1, so 0 stays available as a sentinel.
		let id = txn.insert_into_dictionary(&dict, &value).unwrap();
		assert_eq!(id, DictionaryEntryId::U8(1));
	}

	#[test]
	fn test_insert_duplicate_value() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id1 = txn.insert_into_dictionary(&dict, &value).unwrap();
		let id2 = txn.insert_into_dictionary(&dict, &value).unwrap();

		// Interning is the point: a repeat must reuse the id, not allocate a second one.
		assert_eq!(id1, id2);
		assert_eq!(id1, DictionaryEntryId::U8(1));
	}

	#[test]
	fn test_insert_multiple_values() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();

		let id1 = txn.insert_into_dictionary(&dict, &Value::Utf8("hello".to_string())).unwrap();
		let id2 = txn.insert_into_dictionary(&dict, &Value::Utf8("world".to_string())).unwrap();
		let id3 = txn.insert_into_dictionary(&dict, &Value::Utf8("foo".to_string())).unwrap();

		assert_eq!(id1, DictionaryEntryId::U8(1));
		assert_eq!(id2, DictionaryEntryId::U8(2));
		assert_eq!(id3, DictionaryEntryId::U8(3));
	}

	#[test]
	fn test_get_from_dictionary() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id = txn.insert_into_dictionary(&dict, &value).unwrap();
		let retrieved = txn.get_from_dictionary(&dict, id).unwrap();

		assert_eq!(retrieved, Some(value));
	}

	#[test]
	fn test_get_nonexistent_id() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();

		// An unknown id must be none, not an error or a garbage entry.
		let retrieved = txn.get_from_dictionary(&dict, DictionaryEntryId::U8(999)).unwrap();
		assert_eq!(retrieved, None);
	}

	#[test]
	fn test_find_in_dictionary() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id = txn.insert_into_dictionary(&dict, &value).unwrap();

		// The reverse index must see an uncommitted insert from the same transaction.
		let found = txn.find_in_dictionary(&dict, &value).unwrap();
		assert_eq!(found, Some(id));
	}

	#[test]
	fn test_find_nonexistent_value() {
		let mut txn = create_test_admin_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("not_inserted".to_string());

		// A lookup miss must not intern the value as a side effect.
		let found = txn.find_in_dictionary(&dict, &value).unwrap();
		assert_eq!(found, None);
	}

	#[test]
	fn test_dictionary_with_uint1_id() {
		let mut txn = create_test_admin_transaction();
		let dict = Dictionary {
			id: DictionaryId(2),
			namespace: NamespaceId::SYSTEM,
			name: "dict_u1".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint1,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U1(1));
		assert_eq!(id.id_type(), ValueType::Uint1);
	}

	#[test]
	fn test_dictionary_with_uint2_id() {
		let mut txn = create_test_admin_transaction();
		let dict = Dictionary {
			id: DictionaryId(3),
			namespace: NamespaceId::SYSTEM,
			name: "dict_u2".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint2,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U2(1));
		assert_eq!(id.id_type(), ValueType::Uint2);
	}

	#[test]
	fn test_dictionary_with_uint4_id() {
		let mut txn = create_test_admin_transaction();
		let dict = Dictionary {
			id: DictionaryId(4),
			namespace: NamespaceId::SYSTEM,
			name: "dict_u4".to_string(),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint4,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U4(1));
		assert_eq!(id.id_type(), ValueType::Uint4);
	}
}
