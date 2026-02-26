// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod user_authentication {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const USER_ID: usize = 1;
	pub(crate) const METHOD: usize = 2;
	pub(crate) const PROPERTIES: usize = 3;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("user_id", Type::Uint8),
			SchemaField::unconstrained("method", Type::Utf8),
			SchemaField::unconstrained("properties", Type::Utf8),
		])
	});
}
