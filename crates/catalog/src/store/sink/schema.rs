// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod sink {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const SOURCE_NAMESPACE: usize = 3;
	pub(crate) const SOURCE_NAME: usize = 4;
	pub(crate) const CONNECTOR: usize = 5;
	pub(crate) const CONFIG: usize = 6;
	pub(crate) const STATUS: usize = 7;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("namespace", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
			RowSchemaField::unconstrained("source_namespace", Type::Uint8),
			RowSchemaField::unconstrained("source_name", Type::Utf8),
			RowSchemaField::unconstrained("connector", Type::Utf8),
			RowSchemaField::unconstrained("config", Type::Utf8),
			RowSchemaField::unconstrained("status", Type::Uint1),
		])
	});
}

pub(crate) mod sink_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("name", Type::Utf8),
		])
	});
}
