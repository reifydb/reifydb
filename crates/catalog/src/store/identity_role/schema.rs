// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod identity_role {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const IDENTITY: usize = 0;
	pub(crate) const ROLE_ID: usize = 1;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("identity", Type::IdentityId),
			SchemaField::unconstrained("role_id", Type::Uint8),
		])
	});
}
