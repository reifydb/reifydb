// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::{FlowDef, FlowId, FlowStatus},
		id::NamespaceId,
	},
	key::{flow::FlowKey, namespace_flow::NamespaceFlowKey},
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	CatalogStore,
	store::flow::schema::{flow, flow_namespace},
};

impl CatalogStore {
	pub(crate) fn find_flow(rx: &mut impl AsTransaction, id: FlowId) -> crate::Result<Option<FlowDef>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&FlowKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowId(flow::SCHEMA.get_u64(&row, flow::ID));
		let namespace = NamespaceId(flow::SCHEMA.get_u64(&row, flow::NAMESPACE));
		let name = flow::SCHEMA.get_utf8(&row, flow::NAME).to_string();
		let status_u8 = flow::SCHEMA.get_u8(&row, flow::STATUS);
		let status = FlowStatus::from_u8(status_u8);

		Ok(Some(FlowDef {
			id,
			name,
			namespace,
			status,
		}))
	}

	pub(crate) fn find_flow_by_name(
		rx: &mut impl AsTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<FlowDef>> {
		let name = name.as_ref();
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(NamespaceFlowKey::full_scan(namespace), 1024)?;

		let mut found_flow = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let flow_name = flow_namespace::SCHEMA.get_utf8(row, flow_namespace::NAME);
			if name == flow_name {
				found_flow = Some(FlowId(flow_namespace::SCHEMA.get_u64(row, flow_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(flow) = found_flow else {
			return Ok(None);
		};

		Ok(Some(Self::get_flow(&mut txn, flow)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_find_flow_by_name_ok() {
		let mut txn = create_test_command_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_two", "flow_two");

		let result = CatalogStore::find_flow_by_name(&mut txn, namespace_two.id, "flow_two").unwrap().unwrap();
		assert_eq!(result.name, "flow_two");
		assert_eq!(result.namespace, namespace_two.id);
	}

	#[test]
	fn test_find_flow_by_name_empty() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "some_flow").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_flow_by_name_not_found() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_flow(&mut txn, "test_namespace", "flow_one");
		create_flow(&mut txn, "test_namespace", "flow_two");

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "flow_three").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_flow_by_name_different_namespace() {
		let mut txn = create_test_command_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_flow(&mut txn, "namespace_one", "my_flow");

		// Flow exists in namespace_one but not in namespace_two
		let result = CatalogStore::find_flow_by_name(&mut txn, namespace_two.id, "my_flow").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_flow_by_name_case_sensitive() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_flow(&mut txn, "test_namespace", "MyFlow");

		// Flow names are case-sensitive
		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "myflow").unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "MyFlow").unwrap();
		assert!(result.is_some());
	}

	#[test]
	fn test_find_flow_by_id() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);

		let flow = create_flow(&mut txn, "test_namespace", "test_flow");

		let result = CatalogStore::find_flow(&mut txn, flow.id).unwrap().unwrap();
		assert_eq!(result.id, flow.id);
		assert_eq!(result.name, "test_flow");
	}

	#[test]
	fn test_find_flow_by_id_not_found() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_flow(&mut txn, 999.into()).unwrap();
		assert!(result.is_none());
	}
}
