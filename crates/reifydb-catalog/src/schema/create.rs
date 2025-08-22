// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
    schema::layout::schema,
    sequence::SystemSequence,
    Catalog,
};
use reifydb_core::interface::LiteCommandTransaction;
use reifydb_core::{
    interface::{
        EncodableKey, SchemaDef, SchemaKey, Transaction,
        VersionedCommandTransaction,
    },
    result::error::diagnostic::catalog::schema_already_exists,
    return_error,
    OwnedFragment,
};

#[derive(Debug, Clone)]
pub struct SchemaToCreate {
	pub schema_fragment: Option<OwnedFragment>,
	pub name: String,
}

impl Catalog {
	pub fn create_schema(
		&self,
		txn: &mut impl LiteCommandTransaction,
		to_create: SchemaToCreate,
	) -> crate::Result<SchemaDef> {
		if let Some(schema) =
			self.find_schema_by_name(txn, &to_create.name)?
		{
			return_error!(schema_already_exists(
				to_create.schema_fragment,
				&schema.name
			));
		}

		let schema_id = SystemSequence::next_schema_id(txn)?;

		let mut row = schema::LAYOUT.allocate_row();
		schema::LAYOUT.set_u64(&mut row, schema::ID, schema_id);
		schema::LAYOUT.set_utf8(
			&mut row,
			schema::NAME,
			&to_create.name,
		);

		txn.set(
			&SchemaKey {
				schema: schema_id,
			}
			.encode(),
			row,
		)?;

		Ok(self.get_schema(txn, schema_id)?)
	}
}

#[cfg(test)]
mod tests {
    use reifydb_core::interface::SchemaId;
    use reifydb_transaction::test_utils::create_test_command_transaction;

    use crate::{schema::create::SchemaToCreate, Catalog};

    #[test]
	fn test_create_schema() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();

		let to_create = SchemaToCreate {
			schema_fragment: None,
			name: "test_schema".to_string(),
		};

		// First creation should succeed
		let result =
			catalog.create_schema(&mut txn, to_create.clone())
				.unwrap();
		assert_eq!(result.id, SchemaId(1025));
		assert_eq!(result.name, "test_schema");

		// Creating the same schema again with `if_not_exists = false`
		// should return error
		let err = catalog.create_schema(&mut txn, to_create)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_001");
	}
}
