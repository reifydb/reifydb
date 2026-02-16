// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod reducer {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KEY_COLUMNS: usize = 3;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("namespace", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("key_columns", Type::Utf8),
		])
	});
}

pub(crate) mod reducer_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
		])
	});
}

pub(crate) mod reducer_action {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const REDUCER: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const DATA: usize = 3;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("reducer", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("data", Type::Blob),
		])
	});
}
