// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod token {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const TOKEN: usize = 1;
	pub(crate) const IDENTITY: usize = 2;
	pub(crate) const USER: usize = 3;
	pub(crate) const EXPIRES_AT: usize = 4;
	pub(crate) const CREATED_AT: usize = 5;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("token", Type::Utf8),
			SchemaField::unconstrained("identity", Type::IdentityId),
			SchemaField::unconstrained("user", Type::Uint8),
			SchemaField::unconstrained("expires_at", Type::DateTime),
			SchemaField::unconstrained("created_at", Type::DateTime),
		])
	});
}
