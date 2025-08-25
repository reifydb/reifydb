// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, QueryTransaction, SchemaId, SchemaViewKey, Versioned,
	ViewDef, ViewId, ViewKey, ViewKind,
};

use crate::{
	CatalogStore,
	view::layout::{view, view_schema},
};

impl CatalogStore {
	pub fn find_view(
		rx: &mut impl QueryTransaction,
		id: ViewId,
	) -> crate::Result<Option<ViewDef>> {
		let Some(versioned) = rx.get(&ViewKey {
			view: id,
		}
		.encode())?
		else {
			return Ok(None);
		};

		let row = versioned.row;
		let id = ViewId(view::LAYOUT.get_u64(&row, view::ID));
		let schema = SchemaId(view::LAYOUT.get_u64(&row, view::SCHEMA));
		let name = view::LAYOUT.get_utf8(&row, view::NAME).to_string();

		let kind = match view::LAYOUT.get_u8(&row, view::KIND) {
			0 => ViewKind::Deferred,
			1 => ViewKind::Transactional,
			_ => unimplemented!(),
		};

		Ok(Some(ViewDef {
			id,
			name,
			schema,
			kind,
			columns: Self::list_view_columns(rx, id)?,
		}))
	}

	pub fn find_view_by_name(
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

		Ok(Some(Self::get_view(rx, view)?))
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

		let result = CatalogStore::find_view_by_name(
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

		let result = CatalogStore::find_view_by_name(
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

		let result = CatalogStore::find_view_by_name(
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

		let result = CatalogStore::find_view_by_name(
			&mut txn,
			SchemaId(2),
			"view_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
