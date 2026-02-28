// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, sumtype::SumTypeDef},
	key::{namespace_sumtype::NamespaceSumTypeKey, sumtype::SumTypeKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use super::sumtype_def_from_row;
use crate::{CatalogStore, Result, store::sumtype::schema::sumtype_namespace};

impl CatalogStore {
	pub(crate) fn find_sumtype(rx: &mut Transaction<'_>, sumtype_id: SumTypeId) -> Result<Option<SumTypeDef>> {
		let Some(multi) = rx.get(&SumTypeKey::encoded(sumtype_id))? else {
			return Ok(None);
		};

		Ok(Some(sumtype_def_from_row(&multi.values)))
	}

	pub(crate) fn find_sumtype_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<SumTypeDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSumTypeKey::full_scan(namespace), 1024)?;

		let mut found_id = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let entry_name = sumtype_namespace::SCHEMA.get_utf8(row, sumtype_namespace::NAME);
			if name == entry_name {
				found_id =
					Some(SumTypeId(sumtype_namespace::SCHEMA.get_u64(row, sumtype_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(id) = found_id else {
			return Ok(None);
		};

		Ok(Some(Self::get_sumtype(rx, id)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::NamespaceId, sumtype::SumTypeKind};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::sumtype::SumTypeId;

	use crate::{
		CatalogStore,
		test_utils::{create_event, create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_event(&mut txn, "namespace_one", "event_one", vec![]);
		create_event(&mut txn, "namespace_two", "event_two", vec![]);
		create_event(&mut txn, "namespace_three", "event_three", vec![]);

		let result = CatalogStore::find_sumtype_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1027),
			"event_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, SumTypeId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "event_two");
		assert_eq!(result.kind, SumTypeKind::Event);
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_sumtype_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"some_event",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_event() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_event(&mut txn, "namespace_one", "event_one", vec![]);
		create_event(&mut txn, "namespace_two", "event_two", vec![]);
		create_event(&mut txn, "namespace_three", "event_three", vec![]);

		// test_namespace (NamespaceId(1025)) has no events
		let result = CatalogStore::find_sumtype_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
			"event_nonexistent",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_event(&mut txn, "namespace_one", "event_one", vec![]);
		create_event(&mut txn, "namespace_two", "event_two", vec![]);
		create_event(&mut txn, "namespace_three", "event_three", vec![]);

		// event_two is in NamespaceId(1027), not NamespaceId(2)
		let result = CatalogStore::find_sumtype_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(2),
			"event_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
