// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const PARENT_ID: usize = 2;
	pub(crate) const GRPC: usize = 3;
	pub(crate) const LOCAL_NAME: usize = 4;
	pub(crate) const TOKEN: usize = 5;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("id", Type::Uint8),
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("parent_id", Type::Uint8),
			SchemaField::unconstrained("grpc", Type::Utf8),
			SchemaField::unconstrained("local_name", Type::Utf8),
			SchemaField::unconstrained("token", Type::Utf8),
		])
	});
}
