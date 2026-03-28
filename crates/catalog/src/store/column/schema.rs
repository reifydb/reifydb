// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const PRIMITIVE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const VALUE: usize = 3;
	pub(crate) const INDEX: usize = 4;
	pub(crate) const AUTO_INCREMENT: usize = 5;
	pub(crate) const CONSTRAINT: usize = 6;
	pub(crate) const DICTIONARY_ID: usize = 7; // 0 = no dictionary, else dictionary_id

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("primitive", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("value", Type::Uint1),
			RowSchemaField::unconstrained("index", Type::Uint1),
			RowSchemaField::unconstrained("auto_increment", Type::Boolean),
			RowSchemaField::unconstrained("constraint", Type::Blob),
			RowSchemaField::unconstrained("dictionary_id", Type::Uint8),
		])
	});
}

pub(crate) mod primitive_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const INDEX: usize = 2;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("index", Type::Uint1),
		])
	});
}
