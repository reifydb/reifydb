// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod column_property {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const COLUMN: usize = 1;
	pub(crate) const POLICY: usize = 2;
	pub(crate) const VALUE: usize = 3;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("column", Type::Uint8),
			RowSchemaField::unconstrained("policy", Type::Uint1),
			RowSchemaField::unconstrained("value", Type::Uint1),
		])
	});
}
