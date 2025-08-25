// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{QueryTransaction, ViewDef, ViewId},
	internal_error,
};

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_view(
		rx: &mut impl QueryTransaction,
		view: ViewId,
	) -> crate::Result<ViewDef> {
		CatalogStore::find_view(rx, view)?.ok_or_else(|| {
			Error(internal_error!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				view
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SchemaId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_schema, create_view, ensure_test_schema},
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

		let result =
			CatalogStore::get_view(&mut txn, ViewId(1026)).unwrap();

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

		let err = CatalogStore::get_view(&mut txn, ViewId(42))
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ViewId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
