// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::NamespaceId,
		reducer::{ReducerActionDef, ReducerDef, ReducerId},
	},
	key::{namespace_reducer::NamespaceReducerKey, reducer::ReducerKey, reducer_action::ReducerActionKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	store::{
		reducer::schema::{reducer, reducer_action, reducer_namespace},
		sequence::reducer::next_reducer_id,
	},
};

#[derive(Debug, Clone)]
pub struct ReducerToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub key_columns: Vec<String>,
}

impl CatalogStore {
	pub(crate) fn create_reducer(
		txn: &mut AdminTransaction,
		to_create: ReducerToCreate,
	) -> crate::Result<ReducerDef> {
		let namespace_id = to_create.namespace;

		let reducer_id = next_reducer_id(txn)?;
		Self::store_reducer(txn, reducer_id, namespace_id, &to_create)?;
		Self::link_reducer_to_namespace(txn, namespace_id, reducer_id, to_create.name.text())?;

		Ok(ReducerDef {
			id: reducer_id,
			namespace: namespace_id,
			name: to_create.name.text().to_string(),
			key_columns: to_create.key_columns,
		})
	}

	fn store_reducer(
		txn: &mut AdminTransaction,
		id: ReducerId,
		namespace: NamespaceId,
		to_create: &ReducerToCreate,
	) -> crate::Result<()> {
		let mut row = reducer::SCHEMA.allocate();
		reducer::SCHEMA.set_u64(&mut row, reducer::ID, id);
		reducer::SCHEMA.set_u64(&mut row, reducer::NAMESPACE, namespace);
		reducer::SCHEMA.set_utf8(&mut row, reducer::NAME, to_create.name.text());
		// Store key columns as comma-separated string
		let key_columns_str = to_create.key_columns.join(",");
		reducer::SCHEMA.set_utf8(&mut row, reducer::KEY_COLUMNS, &key_columns_str);

		let key = ReducerKey::encoded(id);
		txn.set(&key, row)?;

		Ok(())
	}

	fn link_reducer_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		reducer_id: ReducerId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = reducer_namespace::SCHEMA.allocate();
		reducer_namespace::SCHEMA.set_u64(&mut row, reducer_namespace::ID, reducer_id);
		reducer_namespace::SCHEMA.set_utf8(&mut row, reducer_namespace::NAME, name);
		let key = NamespaceReducerKey::encoded(namespace, reducer_id);
		txn.set(&key, row)?;
		Ok(())
	}

	pub(crate) fn create_reducer_action(
		txn: &mut AdminTransaction,
		action_def: &ReducerActionDef,
	) -> crate::Result<()> {
		let mut row = reducer_action::SCHEMA.allocate();
		reducer_action::SCHEMA.set_u64(&mut row, reducer_action::ID, action_def.id);
		reducer_action::SCHEMA.set_u64(&mut row, reducer_action::REDUCER, action_def.reducer);
		reducer_action::SCHEMA.set_utf8(&mut row, reducer_action::NAME, &action_def.name);
		reducer_action::SCHEMA.set_blob(&mut row, reducer_action::DATA, &action_def.data);

		txn.set(&ReducerActionKey::encoded(action_def.id), row)?;

		Ok(())
	}

	pub(crate) fn delete_reducer_action(
		txn: &mut AdminTransaction,
		action_def: &ReducerActionDef,
	) -> crate::Result<()> {
		txn.remove(&ReducerActionKey::encoded(action_def.id))?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{interface::catalog::reducer::ReducerId, key::namespace_reducer::NamespaceReducerKey};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::reducer::{create::ReducerToCreate, schema::reducer_namespace},
		test_utils::{create_namespace, create_reducer, create_reducer_action, ensure_test_namespace},
	};

	#[test]
	fn test_create_reducer() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::create_reducer(
			&mut txn,
			ReducerToCreate {
				name: Fragment::internal("test_reducer"),
				namespace: test_namespace.id,
				key_columns: vec![],
			},
		)
		.unwrap();

		assert_eq!(result.id, ReducerId(1));
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "test_reducer");
		assert!(result.key_columns.is_empty());
	}

	#[test]
	fn test_create_reducer_with_key_columns() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::create_reducer(
			&mut txn,
			ReducerToCreate {
				name: Fragment::internal("keyed_reducer"),
				namespace: test_namespace.id,
				key_columns: vec!["user_id".to_string(), "region".to_string()],
			},
		)
		.unwrap();

		assert_eq!(result.key_columns, vec!["user_id", "region"]);
	}

	#[test]
	fn test_reducer_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_reducer(&mut txn, "test_namespace", "reducer_one", &[]);
		create_reducer(&mut txn, "test_namespace", "reducer_two", &[]);

		let links: Vec<_> = txn
			.range(NamespaceReducerKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let mut found_one = false;
		let mut found_two = false;

		for link in &links {
			let row = &link.values;
			let name = reducer_namespace::SCHEMA.get_utf8(row, reducer_namespace::NAME);

			match name {
				"reducer_one" => found_one = true,
				"reducer_two" => found_two = true,
				_ => panic!("Unexpected reducer name: {}", name),
			}
		}

		assert!(found_one, "reducer_one not found in namespace links");
		assert!(found_two, "reducer_two not found in namespace links");
	}

	#[test]
	fn test_create_reducer_multiple_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		let result_one = create_reducer(&mut txn, "namespace_one", "shared_name", &[]);
		let result_two = create_reducer(&mut txn, "namespace_two", "shared_name", &[]);

		assert_eq!(result_one.name, "shared_name");
		assert_eq!(result_one.namespace, namespace_one.id);
		assert_eq!(result_two.name, "shared_name");
		assert_eq!(result_two.namespace, namespace_two.id);
		assert_ne!(result_one.id, result_two.id);
	}

	#[test]
	fn test_create_reducer_action() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);
		let action = create_reducer_action(&mut txn, reducer.id, "increment", b"action_data");

		let found = CatalogStore::find_reducer_action_by_name(&mut txn, reducer.id, "increment")
			.unwrap()
			.expect("Action not found");
		assert_eq!(found.id, action.id);
		assert_eq!(found.reducer, reducer.id);
		assert_eq!(found.name, "increment");
		assert_eq!(found.data.as_ref(), b"action_data");
	}

	#[test]
	fn test_create_multiple_actions_same_reducer() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);
		create_reducer_action(&mut txn, reducer.id, "action_a", b"data_a");
		create_reducer_action(&mut txn, reducer.id, "action_b", b"data_b");

		let actions = CatalogStore::list_reducer_actions(&mut txn, reducer.id).unwrap();
		assert_eq!(actions.len(), 2);
	}

	#[test]
	fn test_delete_reducer_action() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);
		let action = create_reducer_action(&mut txn, reducer.id, "to_delete", b"data");

		CatalogStore::delete_reducer_action(&mut txn, &action).unwrap();

		let result = CatalogStore::find_reducer_action_by_name(&mut txn, reducer.id, "to_delete").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_create_actions_different_reducers() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer_a = create_reducer(&mut txn, "test_namespace", "reducer_a", &[]);
		let reducer_b = create_reducer(&mut txn, "test_namespace", "reducer_b", &[]);

		create_reducer_action(&mut txn, reducer_a.id, "shared_action", b"data_a");
		create_reducer_action(&mut txn, reducer_b.id, "shared_action", b"data_b");

		let found_a = CatalogStore::find_reducer_action_by_name(&mut txn, reducer_a.id, "shared_action")
			.unwrap()
			.unwrap();
		let found_b = CatalogStore::find_reducer_action_by_name(&mut txn, reducer_b.id, "shared_action")
			.unwrap()
			.unwrap();

		assert_eq!(found_a.data.as_ref(), b"data_a");
		assert_eq!(found_b.data.as_ref(), b"data_b");
		assert_ne!(found_a.id, found_b.id);
	}
}
