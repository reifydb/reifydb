// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod column_policy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const COLUMN: usize = 1;
	pub(crate) const POLICY: usize = 2;
	pub(crate) const VALUE: usize = 3;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("column", Type::Uint8),
			SchemaField::unconstrained("policy", Type::Uint1),
			SchemaField::unconstrained("value", Type::Uint1),
		])
	});
}
