// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::NamespaceId,
	key::{
		EncodableKey, namespace::NamespaceKey, namespace_dictionary::NamespaceDictionaryKey,
		namespace_flow::NamespaceFlowKey, namespace_ringbuffer::NamespaceRingBufferKey,
		namespace_sumtype::NamespaceSumTypeKey, namespace_table::NamespaceTableKey,
		namespace_view::NamespaceViewKey,
	},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_namespace(txn: &mut AdminTransaction, namespace: NamespaceId) -> crate::Result<()> {
		// Cascade-drop all child objects within this namespace

		// Drop all tables
		{
			let range = NamespaceTableKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut table_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceTableKey::decode(&entry.key) {
					table_ids.push(key.table);
				}
			}
			drop(stream);
			for table_id in table_ids {
				Self::drop_table(txn, table_id)?;
			}
		}

		// Drop all views
		{
			let range = NamespaceViewKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut view_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceViewKey::decode(&entry.key) {
					view_ids.push(key.view);
				}
			}
			drop(stream);
			for view_id in view_ids {
				Self::drop_view(txn, view_id)?;
			}
		}

		// Drop all ringbuffers
		{
			let range = NamespaceRingBufferKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut rb_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceRingBufferKey::decode(&entry.key) {
					rb_ids.push(key.ringbuffer);
				}
			}
			drop(stream);
			for rb_id in rb_ids {
				Self::drop_ringbuffer(txn, rb_id)?;
			}
		}

		// Drop all flows
		{
			let range = NamespaceFlowKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut flow_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceFlowKey::decode(&entry.key) {
					flow_ids.push(key.flow);
				}
			}
			drop(stream);
			for flow_id in flow_ids {
				Self::drop_flow(txn, flow_id)?;
			}
		}

		// Drop all dictionaries
		{
			let range = NamespaceDictionaryKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut dict_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceDictionaryKey::decode(&entry.key) {
					dict_ids.push(key.dictionary);
				}
			}
			drop(stream);
			for dict_id in dict_ids {
				Self::drop_dictionary(txn, dict_id)?;
			}
		}

		// Drop all sumtypes
		{
			let range = NamespaceSumTypeKey::full_scan(namespace);
			let mut stream = txn.range(range, 1024)?;
			let mut st_ids = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = NamespaceSumTypeKey::decode(&entry.key) {
					st_ids.push(key.sumtype);
				}
			}
			drop(stream);
			for st_id in st_ids {
				Self::drop_sumtype(txn, st_id)?;
			}
		}

		// Delete the namespace metadata
		txn.remove(&NamespaceKey::encoded(namespace))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{fragment::Fragment, value::r#type::Type};

	use crate::{
		CatalogStore,
		store::{dictionary::create::DictionaryToCreate, namespace::create::NamespaceToCreate},
		test_utils::{create_flow, create_namespace, create_sumtype, create_table, create_view},
	};

	#[test]
	fn test_drop_namespace() {
		let mut txn = create_test_admin_transaction();

		let created = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: Some(Fragment::internal("test_ns".to_string())),
				name: "test_ns".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut txn), "test_ns").unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::drop_namespace(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut txn), "test_ns").unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_namespace() {
		let mut txn = create_test_admin_transaction();

		// Deleting a non-existent namespace should not error
		let non_existent = NamespaceId(999999);
		let result = CatalogStore::drop_namespace(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_namespace_cascades_to_children() {
		let mut txn = create_test_admin_transaction();

		// Create namespace with child objects
		let ns = create_namespace(&mut txn, "cascade_ns");

		create_table(&mut txn, "cascade_ns", "child_table", &[]);
		create_view(&mut txn, "cascade_ns", "child_view", &[]);
		create_flow(&mut txn, "cascade_ns", "child_flow");
		create_sumtype(&mut txn, "cascade_ns", "child_sumtype", vec![]);
		let dict = CatalogStore::create_dictionary(
			&mut txn,
			DictionaryToCreate {
				namespace: ns.id,
				name: Fragment::internal("child_dict"),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			},
		)
		.unwrap();

		// Verify all children exist before drop
		assert!(CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_table")
			.unwrap()
			.is_some());
		assert!(CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_view")
			.unwrap()
			.is_some());
		assert!(CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_flow")
			.unwrap()
			.is_some());
		assert!(CatalogStore::find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_sumtype")
			.unwrap()
			.is_some());
		assert!(CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), dict.id).unwrap().is_some());

		// Drop the namespace
		CatalogStore::drop_namespace(&mut txn, ns.id).unwrap();

		// Verify namespace is gone
		assert!(CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut txn), "cascade_ns")
			.unwrap()
			.is_none());

		// Verify all children are gone
		assert!(CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_table")
			.unwrap()
			.is_none());
		assert!(CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_view")
			.unwrap()
			.is_none());
		assert!(CatalogStore::find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_flow")
			.unwrap()
			.is_none());
		assert!(CatalogStore::find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns.id, "child_sumtype")
			.unwrap()
			.is_none());
		assert!(CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), dict.id).unwrap().is_none());
	}
}
