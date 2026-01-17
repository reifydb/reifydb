// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::{FlowDef, FlowId, FlowStatus},
		id::NamespaceId,
	},
	key::{flow::FlowKey, namespace_flow::NamespaceFlowKey},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::{error::diagnostic::catalog::flow_already_exists, fragment::Fragment, return_error};

use crate::{
	CatalogStore,
	store::{
		flow::layout::{flow, flow_namespace},
		sequence::flow::next_flow_id,
	},
};

#[derive(Debug, Clone)]
pub struct FlowToCreate {
	pub fragment: Option<Fragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub status: FlowStatus,
}

impl CatalogStore {
	pub fn create_flow(txn: &mut StandardCommandTransaction, to_create: FlowToCreate) -> crate::Result<FlowDef> {
		let namespace_id = to_create.namespace;

		// Check if flow already exists
		if let Some(_flow) = CatalogStore::find_flow_by_name(txn, namespace_id, &to_create.name)? {
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(flow_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&to_create.name
			));
		}

		let flow_id = next_flow_id(txn)?;
		Self::store_flow(txn, flow_id, namespace_id, &to_create)?;
		Self::link_flow_to_namespace(txn, namespace_id, flow_id, &to_create.name)?;

		Ok(Self::get_flow(txn, flow_id)?)
	}

	fn store_flow(
		txn: &mut StandardCommandTransaction,
		flow: FlowId,
		namespace: NamespaceId,
		to_create: &FlowToCreate,
	) -> crate::Result<()> {
		let mut row = flow::LAYOUT.allocate_deprecated();
		flow::LAYOUT.set_u64(&mut row, flow::ID, flow);
		flow::LAYOUT.set_u64(&mut row, flow::NAMESPACE, namespace);
		flow::LAYOUT.set_utf8(&mut row, flow::NAME, &to_create.name);
		flow::LAYOUT.set_u8(&mut row, flow::STATUS, to_create.status.to_u8());

		let key = FlowKey::encoded(flow);
		txn.set(&key, row)?;

		Ok(())
	}

	fn link_flow_to_namespace(
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceId,
		flow: FlowId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = flow_namespace::LAYOUT.allocate_deprecated();
		flow_namespace::LAYOUT.set_u64(&mut row, flow_namespace::ID, flow);
		flow_namespace::LAYOUT.set_utf8(&mut row, flow_namespace::NAME, name);
		let key = NamespaceFlowKey::encoded(namespace, flow);
		txn.set(&key, row)?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{
			flow::{FlowId, FlowStatus},
			id::NamespaceId,
		},
		key::namespace_flow::NamespaceFlowKey,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		store::flow::{create::FlowToCreate, layout::flow_namespace},
		test_utils::{create_namespace, ensure_test_namespace},
	};

	#[test]
	fn test_create_flow() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = FlowToCreate {
			fragment: None,
			name: "test_flow".to_string(),
			namespace: test_namespace.id,
			status: FlowStatus::Active,
		};

		// First creation should succeed
		let result = CatalogStore::create_flow(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, FlowId(1));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_flow");
		assert_eq!(result.status, FlowStatus::Active);

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_flow(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_030");
	}

	#[test]
	fn test_flow_linked_to_namespace() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Create two flows
		let to_create = FlowToCreate {
			fragment: None,
			name: "flow_one".to_string(),
			namespace: test_namespace.id,
			status: FlowStatus::Active,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		let to_create = FlowToCreate {
			fragment: None,
			name: "flow_two".to_string(),
			namespace: test_namespace.id,
			status: FlowStatus::Paused,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		// Verify both are linked to namespace
		let links: Vec<_> = txn
			.range(NamespaceFlowKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Verify link metadata (order may vary)
		let mut found_flow_one = false;
		let mut found_flow_two = false;

		for link in &links {
			let row = &link.values;
			let id = flow_namespace::LAYOUT.get_u64(row, flow_namespace::ID);
			let name = flow_namespace::LAYOUT.get_utf8(row, flow_namespace::NAME);

			match name {
				"flow_one" => {
					assert_eq!(id, 1);
					found_flow_one = true;
				}
				"flow_two" => {
					assert_eq!(id, 2);
					found_flow_two = true;
				}
				_ => panic!("Unexpected flow name: {}", name),
			}
		}

		assert!(found_flow_one, "flow_one not found in namespace links");
		assert!(found_flow_two, "flow_two not found in namespace links");
	}

	#[test]
	fn test_create_flow_multiple_namespaces() {
		let mut txn = create_test_command_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		// Create flow in first namespace
		let to_create = FlowToCreate {
			fragment: None,
			name: "shared_name".to_string(),
			namespace: namespace_one.id,
			status: FlowStatus::Active,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		// Should be able to create flow with same name in different namespace
		let to_create = FlowToCreate {
			fragment: None,
			name: "shared_name".to_string(),
			namespace: namespace_two.id,
			status: FlowStatus::Active,
		};
		let result = CatalogStore::create_flow(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id);
	}
}
