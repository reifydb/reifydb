// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod flow_edge {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const SOURCE: usize = 2;
	pub(crate) const TARGET: usize = 3;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("id", Type::Uint8),
			RowSchemaField::unconstrained("flow", Type::Uint8),
			RowSchemaField::unconstrained("source", Type::Uint8),
			RowSchemaField::unconstrained("target", Type::Uint8),
		])
	});
}

pub(crate) mod flow_edge_by_flow {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{RowSchema, RowSchemaField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const FLOW: usize = 0;
	pub(crate) const ID: usize = 1;

	pub(crate) static SCHEMA: Lazy<RowSchema> = Lazy::new(|| {
		RowSchema::new(vec![
			RowSchemaField::unconstrained("flow", Type::Uint8),
			RowSchemaField::unconstrained("id", Type::Uint8),
		])
	});
}
