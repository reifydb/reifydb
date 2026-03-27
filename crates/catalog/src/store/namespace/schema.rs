// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const PARENT_ID: usize = 2;
	pub(crate) const GRPC: usize = 3;
	pub(crate) const LOCAL_NAME: usize = 4;
	pub(crate) const TOKEN: usize = 5;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("parent_id", Type::Uint8),
			RowSchemaField::unconstrained("grpc", Type::Utf8),
			RowSchemaField::unconstrained("local_name", Type::Utf8),
			RowSchemaField::unconstrained("token", Type::Utf8),
		])
	});
}
