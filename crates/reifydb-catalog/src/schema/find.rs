// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	schema::{convert_schema, layout::schema},
	Catalog,
};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
	interface::{SchemaDef, SchemaKey, VersionedQueryTransaction},
	row::EncodedRow,
};

impl Catalog {
	pub fn find_schema_by_name(
		&self,
		rx: &mut impl QueryTransaction,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>> {
		let name = name.as_ref();
		Ok(rx.range(SchemaKey::full_scan())?.find_map(|versioned| {
			let row: &EncodedRow = &versioned.row;
			let schema_name =
				schema::LAYOUT.get_utf8(row, schema::NAME);
			if name == schema_name {
				Some(convert_schema(versioned))
			} else {
				None
			}
		}))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::{schema::SchemaId, test_utils::create_schema, Catalog};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		create_schema(&mut txn, "test_schema");

		let schema = catalog
			.find_schema_by_name(&mut txn, "test_schema")
			.unwrap()
			.unwrap();

		assert_eq!(schema.id, SchemaId(1025));
		assert_eq!(schema.name, "test_schema");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		let result = catalog
			.find_schema_by_name(&mut txn, "test_schema")
			.unwrap();

		assert_eq!(result, None);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		create_schema(&mut txn, "another_schema");

		let result = catalog
			.find_schema_by_name(&mut txn, "test_schema")
			.unwrap();
		assert_eq!(result, None);
	}
}
