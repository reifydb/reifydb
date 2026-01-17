// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const PRIMITIVE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;
	pub(crate) const AUTO_INCREMENT: usize = 5;
	pub(crate) const CONSTRAINT: usize = 6;
	pub(crate) const DICTIONARY_ID: usize = 7; // 0 = no dictionary, else dictionary_id

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("primitive", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("value", Type::Uint1),
			SchemaField::unconstrained("index", Type::Uint1),
			SchemaField::unconstrained("auto_increment", Type::Boolean),
			SchemaField::unconstrained("constraint", Type::Blob),
			SchemaField::unconstrained("dictionary_id", Type::Uint8),
		])
	});
}

pub(crate) mod primitive_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const INDEX: usize = 2;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("index", Type::Uint1),
		])
	});
}
