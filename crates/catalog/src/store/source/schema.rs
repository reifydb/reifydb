// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod source {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const CONNECTOR: usize = 3;
	pub(crate) const CONFIG: usize = 4;
	pub(crate) const TARGET_NAMESPACE: usize = 5;
	pub(crate) const TARGET_NAME: usize = 6;
	pub(crate) const STATUS: usize = 7;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("namespace", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("connector", Type::Utf8),
			SchemaField::unconstrained("config", Type::Utf8),
			SchemaField::unconstrained("target_namespace", Type::Uint8),
			SchemaField::unconstrained("target_name", Type::Utf8),
			SchemaField::unconstrained("status", Type::Uint1),
		])
	});
}

pub(crate) mod source_namespace {
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
