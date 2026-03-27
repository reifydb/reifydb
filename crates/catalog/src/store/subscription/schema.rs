// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const ACKNOWLEDGED_VERSION: usize = 1;
	pub const PRIMARY_KEY: usize = 2;

	pub static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("acknowledged_version", Type::Uint8),
			RowSchemaField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub mod subscription_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const NAME: usize = 1;
	pub const TYPE: usize = 2;

	pub static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("type", Type::Uint1),
		])
	});
}
