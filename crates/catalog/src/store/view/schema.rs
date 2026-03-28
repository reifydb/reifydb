// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod view {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KIND: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;
	pub(crate) const STORAGE_KIND: usize = 5;
	pub(crate) const UNDERLYING_PRIMITIVE_ID: usize = 6;
	pub(crate) const CAPACITY: usize = 7;
	pub(crate) const PROPAGATE_EVICTIONS: usize = 8;
	pub(crate) const KEY_COLUMN: usize = 9;
	pub(crate) const KEY_KIND: usize = 10;
	pub(crate) const PRECISION: usize = 11;
	pub(crate) const TAG_ID: usize = 12;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("namespace", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("kind", Type::Uint1),
			RowSchemaField::unconstrained("primary_key", Type::Uint8),
			RowSchemaField::unconstrained("storage_kind", Type::Uint1),
			RowSchemaField::unconstrained("underlying_object_id", Type::Uint8),
			RowSchemaField::unconstrained("capacity", Type::Uint8),
			RowSchemaField::unconstrained("propagate_evictions", Type::Uint1),
			RowSchemaField::unconstrained("key_column", Type::Utf8),
			RowSchemaField::unconstrained("key_kind", Type::Uint1),
			RowSchemaField::unconstrained("precision", Type::Uint1),
			RowSchemaField::unconstrained("tag_id", Type::Uint8),
		])
	});
}

pub(crate) mod view_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
		])
	});
}
