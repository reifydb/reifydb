// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod security_policy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const TARGET_TYPE: usize = 2;
	pub(crate) const TARGET_NAMESPACE: usize = 3;
	pub(crate) const TARGET_OBJECT: usize = 4;
	pub(crate) const ENABLED: usize = 5;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("target_type", Type::Utf8),
			SchemaField::unconstrained("target_namespace", Type::Utf8),
			SchemaField::unconstrained("target_object", Type::Utf8),
			SchemaField::unconstrained("enabled", Type::Boolean),
		])
	});
}

pub(crate) mod security_policy_op {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const POLICY_ID: usize = 0;
	pub(crate) const OPERATION: usize = 1;
	pub(crate) const BODY_SOURCE: usize = 2;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("policy_id", Type::Uint8),
			SchemaField::unconstrained("operation", Type::Utf8),
			SchemaField::unconstrained("body_source", Type::Utf8),
		])
	});
}
