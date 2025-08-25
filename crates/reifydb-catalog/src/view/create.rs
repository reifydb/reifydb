// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use ViewKind::{Deferred, Transactional};
use reifydb_core::{
	OwnedFragment, Type,
	interface::{
		CommandTransaction, EncodableKey, Key, SchemaId, SchemaViewKey,
		ViewDef, ViewId, ViewKey, ViewKind,
	},
	result::error::diagnostic::catalog::{
		schema_not_found, view_already_exists,
	},
	return_error,
};

use crate::{
	CatalogStore,
	sequence::SystemSequence,
	view::layout::{view, view_schema},
	view_column::ColumnIndex,
};

#[derive(Debug, Clone)]
pub struct ViewColumnToCreate {
	pub name: String,
	pub ty: Type,
	pub fragment: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct ViewToCreate {
	pub fragment: Option<OwnedFragment>,
	pub view: String,
	pub schema: String,
	pub columns: Vec<ViewColumnToCreate>,
}

impl CatalogStore {
	pub fn create_deferred_view(
		txn: &mut impl CommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		Self::create_view(txn, to_create, Deferred)
	}

	pub fn create_transactional_view(
		txn: &mut impl CommandTransaction,
		to_create: ViewToCreate,
	) -> crate::Result<ViewDef> {
		Self::create_view(txn, to_create, Transactional)
	}

	fn create_view(
		txn: &mut impl CommandTransaction,
		to_create: ViewToCreate,
		kind: ViewKind,
	) -> crate::Result<ViewDef> {
		let Some(schema) =
			Self::find_schema_by_name(txn, &to_create.schema)?
		else {
			return_error!(schema_not_found(
				to_create.fragment,
				&to_create.schema
			));
		};

		if let Some(view) = Self::find_view_by_name(
			txn,
			schema.id,
			&to_create.view,
		)? {
			return_error!(view_already_exists(
				to_create.fragment,
				&schema.name,
				&view.name
			));
		}

		let view_id = SystemSequence::next_view_id(txn)?;
		Self::store_view(txn, view_id, schema.id, &to_create, kind)?;
		Self::link_view_to_schema(
			txn,
			schema.id,
			view_id,
			&to_create.view,
		)?;

		Self::insert_columns_for_view(txn, view_id, to_create)?;

		Ok(Self::get_view(txn, view_id)?)
	}

	fn store_view(
		txn: &mut impl CommandTransaction,
		view: ViewId,
		schema: SchemaId,
		to_create: &ViewToCreate,
		kind: ViewKind,
	) -> crate::Result<()> {
		let mut row = view::LAYOUT.allocate_row();
		view::LAYOUT.set_u64(&mut row, view::ID, view);
		view::LAYOUT.set_u64(&mut row, view::SCHEMA, schema);
		view::LAYOUT.set_utf8(&mut row, view::NAME, &to_create.view);
		view::LAYOUT.set_u8(
			&mut row,
			view::KIND,
			match kind {
				Deferred => 0,
				Transactional => 1,
			},
		);

		txn.set(
			&ViewKey {
				view,
			}
			.encode(),
			row,
		)?;

		Ok(())
	}

	fn link_view_to_schema(
		txn: &mut impl CommandTransaction,
		schema: SchemaId,
		view: ViewId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = view_schema::LAYOUT.allocate_row();
		view_schema::LAYOUT.set_u64(&mut row, view_schema::ID, view);
		view_schema::LAYOUT.set_utf8(&mut row, view_schema::NAME, name);
		txn.set(
			&Key::SchemaView(SchemaViewKey {
				schema,
				view,
			})
			.encode(),
			row,
		)?;
		Ok(())
	}

	fn insert_columns_for_view(
		txn: &mut impl CommandTransaction,
		view: ViewId,
		to_create: ViewToCreate,
	) -> crate::Result<()> {
		for (idx, column_to_create) in
			to_create.columns.into_iter().enumerate()
		{
			Self::create_view_column(
				txn,
				view,
				crate::view_column::ViewColumnToCreate {
					fragment: column_to_create
						.fragment
						.clone(),
					schema_name: &to_create.schema,
					view,
					view_name: &to_create.view,
					column: column_to_create.name,
					value: column_to_create.ty,
					if_not_exists: false,
					index: ColumnIndex(idx as u16),
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		SchemaId, SchemaViewKey, VersionedQueryTransaction, ViewId,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::ensure_test_schema,
		view::{ViewToCreate, layout::view_schema},
	};

	#[test]
	fn test_create_deferred_view() {
		let mut txn = create_test_command_transaction();

		ensure_test_schema(&mut txn);

		let to_create = ViewToCreate {
			schema: "test_schema".to_string(),
			view: "test_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_deferred_view(
			&mut txn,
			to_create.clone(),
		)
		.unwrap();
		assert_eq!(result.id, ViewId(1025));
		assert_eq!(result.schema, SchemaId(1025));
		assert_eq!(result.name, "test_view");

		// Creating the same view again with `if_not_exists = false`
		// should return error
		let err =
			CatalogStore::create_deferred_view(&mut txn, to_create)
				.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_view_linked_to_schema() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		let to_create = ViewToCreate {
			schema: "test_schema".to_string(),
			view: "test_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_deferred_view(&mut txn, to_create)
			.unwrap();

		let to_create = ViewToCreate {
			schema: "test_schema".to_string(),
			view: "another_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_deferred_view(&mut txn, to_create)
			.unwrap();

		let links = txn
			.range(SchemaViewKey::full_scan(schema.id))
			.unwrap()
			.collect::<Vec<_>>();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.row;
		assert_eq!(
			view_schema::LAYOUT.get_u64(row, view_schema::ID),
			1025
		);
		assert_eq!(
			view_schema::LAYOUT.get_utf8(row, view_schema::NAME),
			"test_view"
		);

		let link = &links[0];
		let row = &link.row;
		assert_eq!(
			view_schema::LAYOUT.get_u64(row, view_schema::ID),
			1026
		);
		assert_eq!(
			view_schema::LAYOUT.get_utf8(row, view_schema::NAME),
			"another_view"
		);
	}

	#[test]
	fn test_create_deferred_view_missing_schema() {
		let mut txn = create_test_command_transaction();

		let to_create = ViewToCreate {
			schema: "missing_schema".to_string(),
			view: "my_view".to_string(),
			columns: vec![],
			fragment: None,
		};

		let err =
			CatalogStore::create_deferred_view(&mut txn, to_create)
				.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_002");
	}
}
