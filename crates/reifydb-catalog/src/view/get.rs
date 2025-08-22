// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{view::layout::view, Catalog};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
	interface::{
		EncodableKey, SchemaId, VersionedQueryTransaction, ViewDef,
		ViewId, ViewKey, ViewKind,
	},
	internal_error,
	Error,
};

impl Catalog {
	pub fn get_view(
		&self,
		rx: &mut impl QueryTransaction,
		view: ViewId,
	) -> crate::Result<ViewDef> {
		let versioned = rx
            .get(&ViewKey { view }.encode())?
            .ok_or_else(|| {
                Error(internal_error!(
						"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.", 
						view
					))
            })?;

		let row = versioned.row;
		let id = ViewId(view::LAYOUT.get_u64(&row, view::ID));
		let schema = SchemaId(view::LAYOUT.get_u64(&row, view::SCHEMA));
		let name = view::LAYOUT.get_utf8(&row, view::NAME).to_string();

		let kind = match view::LAYOUT.get_u8(&row, view::KIND) {
			0 => ViewKind::Deferred,
			1 => ViewKind::Transactional,
			_ => unimplemented!(),
		};

		Ok(ViewDef {
			id,
			name,
			schema,
			kind,
			columns: self.list_view_columns(rx, id)?,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SchemaId, ViewId};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::{
		test_utils::{create_schema, create_view, ensure_test_schema},
		Catalog,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_view(&mut txn, "schema_one", "view_one", &[]);
		create_view(&mut txn, "schema_two", "view_two", &[]);
		create_view(&mut txn, "schema_three", "view_three", &[]);

		let catalog = Catalog::new();
		let result = catalog.get_view(&mut txn, ViewId(1026)).unwrap();

		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.schema, SchemaId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_view(&mut txn, "schema_one", "view_one", &[]);
		create_view(&mut txn, "schema_two", "view_two", &[]);
		create_view(&mut txn, "schema_three", "view_three", &[]);

		let catalog = Catalog::new();
		let err = catalog.get_view(&mut txn, ViewId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ViewId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
