// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, SchemaId, SchemaViewKey, Versioned,
	VersionedQueryTransaction, ViewDef, ViewId, ViewKey,
};

use crate::{
	Catalog,
	view::layout::{view, view_schema},
};

impl Catalog {
	pub fn get_view_by_name(
		rx: &mut impl VersionedQueryTransaction,
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

		Catalog::get_view(rx, view)
	}

	pub fn get_view(
		rx: &mut impl VersionedQueryTransaction,
		view: ViewId,
	) -> crate::Result<Option<ViewDef>> {
		match rx.get(&ViewKey {
			view,
		}
		.encode())?
		{
			Some(versioned) => {
				let row = versioned.row;
				let id =
					ViewId(view::LAYOUT
						.get_u64(&row, view::ID));
				let schema = SchemaId(
					view::LAYOUT
						.get_u64(&row, view::SCHEMA),
				);
				let name = view::LAYOUT
					.get_utf8(&row, view::NAME)
					.to_string();
				Ok(Some(ViewDef {
					id,
					name,
					schema,
					columns: Catalog::list_view_columns(
						rx, id,
					)?,
				}))
			}
			None => Ok(None),
		}
	}
}

#[cfg(test)]
mod tests {
	mod get_view_by_name {
		use reifydb_core::interface::{SchemaId, ViewId};
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{
			Catalog,
			test_utils::{
				create_schema, create_view, ensure_test_schema,
			},
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
			create_view(
				&mut txn,
				"schema_three",
				"view_three",
				&[],
			);

			let result = Catalog::get_view_by_name(
				&mut txn,
				SchemaId(1027),
				"view_two",
			)
			.unwrap()
			.unwrap();
			assert_eq!(result.id, ViewId(1026));
			assert_eq!(result.schema, SchemaId(1027));
			assert_eq!(result.name, "view_two");
		}

		#[test]
		fn test_empty() {
			let mut txn = create_test_command_transaction();
			let result = Catalog::get_view_by_name(
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
			create_view(
				&mut txn,
				"schema_three",
				"view_three",
				&[],
			);

			let result = Catalog::get_view_by_name(
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
			create_view(
				&mut txn,
				"schema_three",
				"view_three",
				&[],
			);

			let result = Catalog::get_view_by_name(
				&mut txn,
				SchemaId(2),
				"view_two",
			)
			.unwrap();
			assert!(result.is_none());
		}
	}

	mod get_view {
		use reifydb_core::interface::{SchemaId, ViewId};
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{
			Catalog,
			test_utils::{
				create_schema, create_view, ensure_test_schema,
			},
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
			create_view(
				&mut txn,
				"schema_three",
				"view_three",
				&[],
			);

			let result = Catalog::get_view(&mut txn, ViewId(1026))
				.unwrap()
				.unwrap();
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
			create_view(
				&mut txn,
				"schema_three",
				"view_three",
				&[],
			);

			let result = Catalog::get_view(&mut txn, ViewId(42))
				.unwrap();
			assert!(result.is_none());
		}
	}
}
