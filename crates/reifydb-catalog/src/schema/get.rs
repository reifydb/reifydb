// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
    schema::convert_schema,
    Catalog,
};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
    interface::{EncodableKey, SchemaDef, SchemaId, SchemaKey},
    internal_error,
    Error,
};

impl Catalog {
	pub fn get_schema(
		&self,
		rx: &mut impl QueryTransaction,
		schema: SchemaId,
	) -> crate::Result<SchemaDef> {
		let versioned = rx
			.get(&&SchemaKey {
				schema,
			}.encode())?
			.ok_or_else(|| {
				Error(internal_error!(
						"Schema with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
						schema
					))
			})?;

		Ok(convert_schema(versioned))
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
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		let result =
			catalog.get_schema(&mut txn, SchemaId(1026)).unwrap();

		assert_eq!(result.id, SchemaId(1026));
		assert_eq!(result.name, "schema_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		let err = catalog.get_schema(&mut txn, SchemaId(23))
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("SchemaId(23)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
