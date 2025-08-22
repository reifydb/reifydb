// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	SchemaId, SchemaViewKey, QueryTransaction, Versioned,
	ViewDef, ViewId,
};

use crate::{view::layout::view_schema, Catalog};

impl Catalog {
	pub fn find_view_by_name(
		&self,
		rx: &mut impl QueryTransaction,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();
		let Some(view) = rx
			.range(SchemaViewKey::full_scan(schema))?
			.find_map(|versioned: Versioned| {
				let row = &versioned.row;
				let view_name = view_schema::LAYOUT
					.get_utf8(row, view_schema::NAME);
				if name == view_name {
					Some(ViewId(view_schema::LAYOUT
						.get_u64(row, view_schema::ID)))
				} else {
					None
				}
			})
		else {
			return Ok(None);
		};

		Ok(Some(self.get_view(rx, view)?))
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
		let result = catalog
			.find_view_by_name(&mut txn, SchemaId(1027), "view_two")
			.unwrap()
			.unwrap();
		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.schema, SchemaId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		let result = catalog
			.find_view_by_name(
				&mut txn,
				SchemaId(1025),
				"some_view",
			)
			.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_view() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_view(&mut txn, "schema_one", "view_one", &[]);
		create_view(&mut txn, "schema_two", "view_two", &[]);
		create_view(&mut txn, "schema_three", "view_three", &[]);

		let catalog = Catalog::new();
		let result = catalog
			.find_view_by_name(
				&mut txn,
				SchemaId(1025),
				"view_four_two",
			)
			.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_schema() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_view(&mut txn, "schema_one", "view_one", &[]);
		create_view(&mut txn, "schema_two", "view_two", &[]);
		create_view(&mut txn, "schema_three", "view_three", &[]);

		let catalog = Catalog::new();
		let result = catalog
			.find_view_by_name(&mut txn, SchemaId(2), "view_two")
			.unwrap();
		assert!(result.is_none());
	}
}
