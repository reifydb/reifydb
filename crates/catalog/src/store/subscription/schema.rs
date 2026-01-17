// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const ACKNOWLEDGED_VERSION: usize = 1;
	pub const PRIMARY_KEY: usize = 2;

	pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uuid7),
			SchemaField::unconstrained("acknowledged_version", Type::Uint8),
			SchemaField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub mod subscription_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const NAME: usize = 1;
	pub const TYPE: usize = 2;

	pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("type", Type::Uint1),
		])
	});
}
