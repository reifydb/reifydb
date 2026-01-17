// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column::ColumnIndex,
		id::{NamespaceId, TableId, ViewId},
		view::{
			ViewDef, ViewKind,
			ViewKind::{Deferred, Transactional},
		},
	},
	key::{namespace_view::NamespaceViewKey, view::ViewKey},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::{
	error::diagnostic::catalog::view_already_exists, fragment::Fragment, return_error,
	value::constraint::TypeConstraint,
};

use crate::{
	CatalogStore,
	store::{
		column::create::ColumnToCreate,
		sequence::system::SystemSequence,
		view::layout::{view, view_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub fragment: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub fragment: Option<Fragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub columns: Vec<ViewColumnToCreate>,
}

impl CatalogStore {
	pub fn create_deferred_view(
		txn: &mut StandardCommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		Self::create_view(txn, to_create, Deferred)
	}

	pub fn create_transactional_view(
		txn: &mut StandardCommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		Self::create_view(txn, to_create, Transactional)
	}

	fn create_view(
		txn: &mut StandardCommandTransaction,
		to_create: ViewToCreate,
		kind: ViewKind,
	) -> crate::Result<ViewDef> {
		let namespace_id = to_create.namespace;

		if let Some(table) = CatalogStore::find_view_by_name(txn, namespace_id, &to_create.name)? {
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(view_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&table.name
			));
		}

		let view_id = SystemSequence::next_view_id(txn)?;
		Self::store_view(txn, view_id, namespace_id, &to_create, kind)?;
		Self::link_view_to_namespace(txn, namespace_id, view_id, &to_create.name)?;

		Self::insert_columns_for_view(txn, view_id, to_create)?;

		Ok(Self::get_view(txn, view_id)?)
	}

	fn store_view(
		txn: &mut StandardCommandTransaction,
		view: ViewId,
		namespace: NamespaceId,
		to_create: &ViewToCreate,
		kind: ViewKind,
	) -> crate::Result<()> {
		let mut row = view::LAYOUT.allocate_deprecated();
		view::LAYOUT.set_u64(&mut row, view::ID, view);
		view::LAYOUT.set_u64(&mut row, view::NAMESPACE, namespace);
		view::LAYOUT.set_utf8(&mut row, view::NAME, &to_create.name);
		view::LAYOUT.set_u8(
			&mut row,
			view::KIND,
			match kind {
				Deferred => 0,
				Transactional => 1,
			},
		);
		view::LAYOUT.set_u64(&mut row, view::PRIMARY_KEY, 0u64); // Initialize with no primary key

		txn.set(&ViewKey::encoded(view), row)?;

		Ok(())
	}

	fn link_view_to_namespace(
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceId,
		view: ViewId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = view_namespace::LAYOUT.allocate_deprecated();
		view_namespace::LAYOUT.set_u64(&mut row, view_namespace::ID, view);
		view_namespace::LAYOUT.set_utf8(&mut row, view_namespace::NAME, name);
		txn.set(&NamespaceViewKey::encoded(namespace, view), row)?;
		Ok(())
	}

	fn insert_columns_for_view(
		txn: &mut StandardCommandTransaction,
		view: ViewId,
		to_create: ViewToCreate,
	) -> crate::Result<()> {
		// Look up namespace name for error messages
		let namespace = Self::get_namespace(txn, to_create.namespace)?;

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				view,
				ColumnToCreate {
					fragment: column_to_create.fragment.clone(),
					namespace_name: namespace.name.clone(),
					table: TableId(view.0), // Convert ViewId to TableId (both are u64)
					table_name: to_create.name.clone(),
					column: column_to_create.name,
					constraint: column_to_create.constraint.clone(),
					if_not_exists: false,
					policies: vec![],
					index: ColumnIndex(idx as u8),
					auto_increment: false,
					dictionary_id: None, // Views don't support dictionaries yet
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, ViewId},
		key::namespace_view::NamespaceViewKey,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		store::view::{create::ViewToCreate, layout::view_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_deferred_view() {
		let mut txn = create_test_command_transaction();

		let namespace = ensure_test_namespace(&mut txn);

		let to_create = ViewToCreate {
			namespace: namespace.id,
			name: "test_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_deferred_view(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, ViewId(1025));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_view");

		let err = CatalogStore::create_deferred_view(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_view_linked_to_namespace() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = ViewToCreate {
			namespace: namespace.id,
			name: "test_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap();

		let to_create = ViewToCreate {
			namespace: namespace.id,
			name: "another_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceViewKey::full_scan(namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.values;
		assert_eq!(view_namespace::LAYOUT.get_u64(row, view_namespace::ID), 1025);
		assert_eq!(view_namespace::LAYOUT.get_utf8(row, view_namespace::NAME), "test_view");

		let link = &links[0];
		let row = &link.values;
		assert_eq!(view_namespace::LAYOUT.get_u64(row, view_namespace::ID), 1026);
		assert_eq!(view_namespace::LAYOUT.get_utf8(row, view_namespace::NAME), "another_view");
	}

	#[test]
	fn test_create_deferred_view_missing_namespace() {
		let mut txn = create_test_command_transaction();

		let to_create = ViewToCreate {
			namespace: NamespaceId(999), // Non-existent namespace
			name: "my_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_deferred_view(&mut txn, to_create).unwrap_err();
	}
}
