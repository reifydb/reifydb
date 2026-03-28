// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod config {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static SCHEMA: Lazy<RowSchema> =
		Lazy::new(|| RowSchema::new(vec![RowSchemaField::unconstrained("value", Type::Any)]));
}
