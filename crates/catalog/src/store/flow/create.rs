// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::flow_already_exists,
	interface::{
		CommandTransaction, EncodableKey, FlowDef, FlowId, FlowKey, FlowStatus, Key, NamespaceFlowKey,
		NamespaceId, SourceId,
	},
	return_error,
};
use reifydb_type::{OwnedFragment, TypeConstraint};

use crate::{
	CatalogStore,
	store::{
		column::{ColumnIndex, ColumnToCreate},
		flow::layout::{flow, flow_namespace},
		sequence::flow::next_flow_id,
	},
};

#[derive(Debug, Clone)]
pub struct FlowColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub fragment: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct FlowToCreate {
	pub fragment: Option<OwnedFragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub query: reifydb_type::Blob,
	pub columns: Vec<FlowColumnToCreate>,
	pub status: FlowStatus,
}

impl CatalogStore {
	pub fn create_flow(txn: &mut impl CommandTransaction, to_create: FlowToCreate) -> crate::Result<FlowDef> {
		let namespace_id = to_create.namespace;

		// Check if flow already exists
		if let Some(_flow) = CatalogStore::find_flow_by_name(txn, namespace_id, &to_create.name)? {
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(flow_already_exists(to_create.fragment, &namespace.name, &to_create.name));
		}

		let flow_id = next_flow_id(txn)?;
		Self::store_flow(txn, flow_id, namespace_id, &to_create)?;
		Self::link_flow_to_namespace(txn, namespace_id, flow_id, &to_create.name)?;
		Self::insert_flow_columns(txn, flow_id, to_create)?;

		Ok(Self::get_flow(txn, flow_id)?)
	}

	fn store_flow(
		txn: &mut impl CommandTransaction,
		flow: FlowId,
		namespace: NamespaceId,
		to_create: &FlowToCreate,
	) -> crate::Result<()> {
		let mut row = flow::LAYOUT.allocate();
		flow::LAYOUT.set_u64(&mut row, flow::ID, flow);
		flow::LAYOUT.set_u64(&mut row, flow::NAMESPACE, namespace);
		flow::LAYOUT.set_utf8(&mut row, flow::NAME, &to_create.name);
		flow::LAYOUT.set_blob(&mut row, flow::QUERY, &to_create.query);
		flow::LAYOUT.set_u8(&mut row, flow::STATUS, to_create.status.to_u8());

		txn.set(
			&FlowKey {
				flow,
			}
			.encode(),
			row,
		)?;

		Ok(())
	}

	fn link_flow_to_namespace(
		txn: &mut impl CommandTransaction,
		namespace: NamespaceId,
		flow: FlowId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = flow_namespace::LAYOUT.allocate();
		flow_namespace::LAYOUT.set_u64(&mut row, flow_namespace::ID, flow);
		flow_namespace::LAYOUT.set_utf8(&mut row, flow_namespace::NAME, name);
		txn.set(
			&Key::NamespaceFlow(NamespaceFlowKey {
				namespace,
				flow,
			})
			.encode(),
			row,
		)?;
		Ok(())
	}

	fn insert_flow_columns(
		txn: &mut impl CommandTransaction,
		flow: FlowId,
		to_create: FlowToCreate,
	) -> crate::Result<()> {
		// Look up namespace name for error messages
		let namespace_name = Self::find_namespace(txn, to_create.namespace)?
			.map(|s| s.name)
			.unwrap_or_else(|| format!("namespace_{}", to_create.namespace));

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				SourceId::Flow(flow),
				ColumnToCreate {
					fragment: column_to_create.fragment.clone(),
					namespace_name: &namespace_name,
					table: flow.0.into(),
					table_name: &to_create.name,
					column: column_to_create.name,
					constraint: column_to_create.constraint.clone(),
					if_not_exists: false,
					policies: vec![],
					index: ColumnIndex(idx as u16),
					auto_increment: false,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		FlowId, FlowStatus, MultiVersionQueryTransaction, NamespaceFlowKey, NamespaceId,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::flow::{
			create::{FlowColumnToCreate, FlowToCreate},
			layout::flow_namespace,
		},
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
			query: reifydb_type::Blob::from(b"FROM test_table".as_slice()),
			columns: vec![],
			status: FlowStatus::Active,
		};

		// First creation should succeed
		let result = CatalogStore::create_flow(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, FlowId(1));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_flow");
		assert_eq!(result.query.as_ref(), b"FROM test_table");
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
			query: reifydb_type::Blob::from(b"MAP 1".as_slice()),
			columns: vec![],
			status: FlowStatus::Active,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		let to_create = FlowToCreate {
			fragment: None,
			name: "flow_two".to_string(),
			namespace: test_namespace.id,
			query: reifydb_type::Blob::from(b"MAP 2".as_slice()),
			columns: vec![],
			status: FlowStatus::Paused,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		// Verify both are linked to namespace
		let links = txn.range(NamespaceFlowKey::full_scan(test_namespace.id)).unwrap().collect::<Vec<_>>();
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
	fn test_create_flow_with_columns() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = FlowToCreate {
			fragment: None,
			name: "flow_with_columns".to_string(),
			namespace: test_namespace.id,
			query: reifydb_type::Blob::from(b"FROM users MAP id, name".as_slice()),
			columns: vec![
				FlowColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					fragment: None,
				},
				FlowColumnToCreate {
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					fragment: None,
				},
			],
			status: FlowStatus::Active,
		};

		let result = CatalogStore::create_flow(&mut txn, to_create).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "id");
		assert_eq!(result.columns[0].constraint.get_type(), Type::Uint8);
		assert_eq!(result.columns[1].name, "name");
		assert_eq!(result.columns[1].constraint.get_type(), Type::Utf8);
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
			query: reifydb_type::Blob::from(b"MAP 1".as_slice()),
			columns: vec![],
			status: FlowStatus::Active,
		};
		CatalogStore::create_flow(&mut txn, to_create).unwrap();

		// Should be able to create flow with same name in different namespace
		let to_create = FlowToCreate {
			fragment: None,
			name: "shared_name".to_string(),
			namespace: namespace_two.id,
			query: reifydb_type::Blob::from(b"MAP 2".as_slice()),
			columns: vec![],
			status: FlowStatus::Active,
		};
		let result = CatalogStore::create_flow(&mut txn, to_create).unwrap();
		assert_eq!(result.name, "shared_name");
		assert_eq!(result.namespace, namespace_two.id);
	}
}
