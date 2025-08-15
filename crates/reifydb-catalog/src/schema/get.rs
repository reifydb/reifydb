// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		EncodableKey, SchemaKey, Versioned, VersionedQueryTransaction,
	},
	row::EncodedRow,
};

use crate::{
	Catalog,
	schema::{SchemaDef, SchemaId, layout::schema},
};

impl Catalog {
	pub fn get_schema_by_name(
		rx: &mut impl VersionedQueryTransaction,
		name: impl AsRef<str>,
	) -> crate::Result<Option<SchemaDef>> {
		let name = name.as_ref();
		Ok(rx.range(SchemaKey::full_scan())?.find_map(|versioned| {
			let row: &EncodedRow = &versioned.row;
			let schema_name =
				schema::LAYOUT.get_utf8(row, schema::NAME);
			if name == schema_name {
				Some(Self::convert_schema(versioned))
			} else {
				None
			}
		}))
	}

	pub fn get_schema(
		rx: &mut impl VersionedQueryTransaction,
		schema: SchemaId,
	) -> crate::Result<Option<SchemaDef>> {
		Ok(rx.get(&SchemaKey {
			schema,
		}
		.encode())?
			.map(Self::convert_schema))
	}

	fn convert_schema(versioned: Versioned) -> SchemaDef {
		let row = versioned.row;
		let id = SchemaId(schema::LAYOUT.get_u64(&row, schema::ID));
		let name =
			schema::LAYOUT.get_utf8(&row, schema::NAME).to_string();

		SchemaDef {
			id,
			name,
		}
	}
}

#[cfg(test)]
mod tests {

	mod get_schema_by_name {
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{
			Catalog, schema::SchemaId, test_utils::create_schema,
		};

		#[test]
		fn test_ok() {
			let mut txn = create_test_command_transaction();
			create_schema(&mut txn, "test_schema");

			let schema = Catalog::get_schema_by_name(
				&mut txn,
				"test_schema",
			)
			.unwrap()
			.unwrap();

			assert_eq!(schema.id, SchemaId(1025));
			assert_eq!(schema.name, "test_schema");
		}

		#[test]
		fn test_empty() {
			let mut txn = create_test_command_transaction();
			let result = Catalog::get_schema_by_name(
				&mut txn,
				"test_schema",
			)
			.unwrap();

			assert_eq!(result, None);
		}

		#[test]
		fn test_not_found() {
			let mut txn = create_test_command_transaction();
			create_schema(&mut txn, "another_schema");

			let result = Catalog::get_schema_by_name(
				&mut txn,
				"test_schema",
			)
			.unwrap();
			assert_eq!(result, None);
		}
	}

	mod get_schema {
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{
			Catalog, schema::SchemaId, test_utils::create_schema,
		};

		#[test]
		fn test_ok() {
			let mut txn = create_test_command_transaction();
			create_schema(&mut txn, "schema_one");
			create_schema(&mut txn, "schema_two");
			create_schema(&mut txn, "schema_three");

			let result =
				Catalog::get_schema(&mut txn, SchemaId(1026))
					.unwrap()
					.unwrap();
			assert_eq!(result.id, SchemaId(1026));
			assert_eq!(result.name, "schema_two");
		}

		#[test]
		fn test_not_found() {
			let mut txn = create_test_command_transaction();
			create_schema(&mut txn, "schema_one");
			create_schema(&mut txn, "schema_two");
			create_schema(&mut txn, "schema_three");

			let result =
				Catalog::get_schema(&mut txn, SchemaId(23))
					.unwrap();
			assert!(result.is_none());
		}
	}
}
