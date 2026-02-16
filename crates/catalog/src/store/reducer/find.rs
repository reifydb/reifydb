// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::NamespaceId,
		reducer::{ReducerActionDef, ReducerActionId, ReducerDef, ReducerId},
	},
	key::{namespace_reducer::NamespaceReducerKey, reducer::ReducerKey, reducer_action::ReducerActionKey},
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	CatalogStore,
	store::reducer::schema::{reducer, reducer_action, reducer_namespace},
};

impl CatalogStore {
	pub(crate) fn find_reducer(rx: &mut impl AsTransaction, id: ReducerId) -> crate::Result<Option<ReducerDef>> {
		let mut txn = rx.as_transaction();
		let Some(multi) = txn.get(&ReducerKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = ReducerId(reducer::SCHEMA.get_u64(&row, reducer::ID));
		let namespace = NamespaceId(reducer::SCHEMA.get_u64(&row, reducer::NAMESPACE));
		let name = reducer::SCHEMA.get_utf8(&row, reducer::NAME).to_string();
		let key_columns_str = reducer::SCHEMA.get_utf8(&row, reducer::KEY_COLUMNS).to_string();
		let key_columns = if key_columns_str.is_empty() {
			vec![]
		} else {
			key_columns_str.split(',').map(|s| s.to_string()).collect()
		};

		Ok(Some(ReducerDef {
			id,
			name,
			namespace,
			key_columns,
		}))
	}

	pub(crate) fn find_reducer_by_name(
		rx: &mut impl AsTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ReducerDef>> {
		let name = name.as_ref();
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(NamespaceReducerKey::full_scan(namespace), 1024)?;

		let mut found_reducer = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let reducer_name = reducer_namespace::SCHEMA.get_utf8(row, reducer_namespace::NAME);
			if name == reducer_name {
				found_reducer =
					Some(ReducerId(reducer_namespace::SCHEMA.get_u64(row, reducer_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(reducer_id) = found_reducer else {
			return Ok(None);
		};

		Self::find_reducer(&mut txn, reducer_id)
	}

	pub(crate) fn find_reducer_action_by_name(
		rx: &mut impl AsTransaction,
		reducer_id: ReducerId,
		name: &str,
	) -> crate::Result<Option<ReducerActionDef>> {
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(ReducerActionKey::full_scan(), 1024)?;

		let mut found = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let action_reducer = ReducerId(reducer_action::SCHEMA.get_u64(row, reducer_action::REDUCER));
			if action_reducer != reducer_id {
				continue;
			}
			let action_name = reducer_action::SCHEMA.get_utf8(row, reducer_action::NAME);
			if name == action_name {
				let id = ReducerActionId(reducer_action::SCHEMA.get_u64(row, reducer_action::ID));
				let data = reducer_action::SCHEMA.get_blob(row, reducer_action::DATA).clone();
				found = Some(ReducerActionDef {
					id,
					reducer: reducer_id,
					name: action_name.to_string(),
					data,
				});
				break;
			}
		}

		Ok(found)
	}

	pub(crate) fn list_reducer_actions(
		rx: &mut impl AsTransaction,
		reducer_id: ReducerId,
	) -> crate::Result<Vec<ReducerActionDef>> {
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(ReducerActionKey::full_scan(), 1024)?;

		let mut actions = Vec::new();
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let action_reducer = ReducerId(reducer_action::SCHEMA.get_u64(row, reducer_action::REDUCER));
			if action_reducer != reducer_id {
				continue;
			}
			let id = ReducerActionId(reducer_action::SCHEMA.get_u64(row, reducer_action::ID));
			let name = reducer_action::SCHEMA.get_utf8(row, reducer_action::NAME).to_string();
			let data = reducer_action::SCHEMA.get_blob(row, reducer_action::DATA).clone();
			actions.push(ReducerActionDef {
				id,
				reducer: reducer_id,
				name,
				data,
			});
		}

		Ok(actions)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::reducer::ReducerId;
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_reducer, create_reducer_action, ensure_test_namespace},
	};

	#[test]
	fn test_find_reducer_by_id() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "test_reducer", &["col_a"]);

		let found = CatalogStore::find_reducer(&mut txn, reducer.id).unwrap().unwrap();
		assert_eq!(found.id, reducer.id);
		assert_eq!(found.namespace, reducer.namespace);
		assert_eq!(found.name, "test_reducer");
		assert_eq!(found.key_columns, vec!["col_a"]);
	}

	#[test]
	fn test_find_reducer_by_id_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_reducer(&mut txn, ReducerId(999)).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_reducer_by_name_ok() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_reducer(&mut txn, "namespace_one", "reducer_one", &[]);
		create_reducer(&mut txn, "namespace_two", "reducer_two", &[]);

		let result =
			CatalogStore::find_reducer_by_name(&mut txn, namespace_two.id, "reducer_two").unwrap().unwrap();
		assert_eq!(result.name, "reducer_two");
		assert_eq!(result.namespace, namespace_two.id);
	}

	#[test]
	fn test_find_reducer_by_name_empty() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_reducer_by_name(&mut txn, test_namespace.id, "some_reducer").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_reducer_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_reducer(&mut txn, "test_namespace", "reducer_one", &[]);
		create_reducer(&mut txn, "test_namespace", "reducer_two", &[]);

		let result = CatalogStore::find_reducer_by_name(&mut txn, test_namespace.id, "reducer_three").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_reducer_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let _namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_reducer(&mut txn, "namespace_one", "my_reducer", &[]);

		let result = CatalogStore::find_reducer_by_name(&mut txn, namespace_two.id, "my_reducer").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_reducer_by_name_case_sensitive() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		create_reducer(&mut txn, "test_namespace", "MyReducer", &[]);

		let result = CatalogStore::find_reducer_by_name(&mut txn, test_namespace.id, "myreducer").unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_reducer_by_name(&mut txn, test_namespace.id, "MyReducer").unwrap();
		assert!(result.is_some());
	}

	#[test]
	fn test_find_reducer_action_by_name_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);
		let action = create_reducer_action(&mut txn, reducer.id, "increment", b"payload");

		let found =
			CatalogStore::find_reducer_action_by_name(&mut txn, reducer.id, "increment").unwrap().unwrap();
		assert_eq!(found.id, action.id);
		assert_eq!(found.reducer, reducer.id);
		assert_eq!(found.name, "increment");
		assert_eq!(found.data.as_ref(), b"payload");
	}

	#[test]
	fn test_find_reducer_action_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);

		let result = CatalogStore::find_reducer_action_by_name(&mut txn, reducer.id, "nonexistent").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_reducer_action_by_name_wrong_reducer() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer_a = create_reducer(&mut txn, "test_namespace", "reducer_a", &[]);
		let reducer_b = create_reducer(&mut txn, "test_namespace", "reducer_b", &[]);

		create_reducer_action(&mut txn, reducer_a.id, "my_action", b"data");

		let result = CatalogStore::find_reducer_action_by_name(&mut txn, reducer_b.id, "my_action").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_list_reducer_actions_empty() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "empty_reducer", &[]);

		let actions = CatalogStore::list_reducer_actions(&mut txn, reducer.id).unwrap();
		assert!(actions.is_empty());
	}

	#[test]
	fn test_list_reducer_actions_multiple() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(&mut txn, "test_namespace", "my_reducer", &[]);
		create_reducer_action(&mut txn, reducer.id, "action_1", b"d1");
		create_reducer_action(&mut txn, reducer.id, "action_2", b"d2");
		create_reducer_action(&mut txn, reducer.id, "action_3", b"d3");

		let actions = CatalogStore::list_reducer_actions(&mut txn, reducer.id).unwrap();
		assert_eq!(actions.len(), 3);

		let names: Vec<&str> = actions.iter().map(|a| a.name.as_str()).collect();
		assert!(names.contains(&"action_1"));
		assert!(names.contains(&"action_2"));
		assert!(names.contains(&"action_3"));
	}

	#[test]
	fn test_list_reducer_actions_filters_by_reducer() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer_a = create_reducer(&mut txn, "test_namespace", "reducer_a", &[]);
		let reducer_b = create_reducer(&mut txn, "test_namespace", "reducer_b", &[]);

		create_reducer_action(&mut txn, reducer_a.id, "action_a1", b"da1");
		create_reducer_action(&mut txn, reducer_a.id, "action_a2", b"da2");
		create_reducer_action(&mut txn, reducer_b.id, "action_b1", b"db1");

		let actions_a = CatalogStore::list_reducer_actions(&mut txn, reducer_a.id).unwrap();
		assert_eq!(actions_a.len(), 2);

		let actions_b = CatalogStore::list_reducer_actions(&mut txn, reducer_b.id).unwrap();
		assert_eq!(actions_b.len(), 1);
		assert_eq!(actions_b[0].name, "action_b1");
	}

	#[test]
	fn test_find_reducer_key_columns_round_trip() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let reducer = create_reducer(
			&mut txn,
			"test_namespace",
			"keyed_reducer",
			&["user_id", "region", "timestamp"],
		);

		let found = CatalogStore::find_reducer(&mut txn, reducer.id).unwrap().unwrap();
		assert_eq!(found.key_columns, vec!["user_id", "region", "timestamp"]);
	}
}
