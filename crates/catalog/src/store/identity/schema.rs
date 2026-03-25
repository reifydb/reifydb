// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod identity {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const IDENTITY: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const ENABLED: usize = 2;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("identity", Type::IdentityId),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("enabled", Type::Boolean),
		])
	});
}
