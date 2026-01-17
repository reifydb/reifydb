// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Schema definitions for schema data storage.

pub(crate) mod schema_header {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	/// Field count index
	pub(crate) const FIELD_COUNT: usize = 0;

	pub(crate) static SCHEMA: Lazy<Schema> =
		Lazy::new(|| Schema::new(vec![SchemaField::unconstrained("field_count", Type::Uint2)]));
}

pub(crate) mod schema_field {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::schema::{Schema, SchemaField};
	use reifydb_type::value::r#type::Type;

	/// Field name index
	pub(crate) const NAME: usize = 0;
	/// Base field type index (Type::to_u8())
	pub(crate) const TYPE: usize = 1;
	/// Constraint type index (0=None, 1=MaxBytes, 2=PrecisionScale)
	pub(crate) const CONSTRAINT_TYPE: usize = 2;
	/// Constraint param 1 (MaxBytes value or precision)
	pub(crate) const CONSTRAINT_P1: usize = 3;
	/// Constraint param 2 (scale for PrecisionScale)
	pub(crate) const CONSTRAINT_P2: usize = 4;
	/// Field offset index
	pub(crate) const OFFSET: usize = 5;
	/// Field size index
	pub(crate) const SIZE: usize = 6;
	/// Field alignment index
	pub(crate) const ALIGN: usize = 7;

	pub(crate) static SCHEMA: Lazy<Schema> = Lazy::new(|| {
		Schema::new(vec![
			SchemaField::unconstrained("name", Type::Utf8),
			SchemaField::unconstrained("base_type", Type::Uint1),
			SchemaField::unconstrained("constraint_type", Type::Uint1),
			SchemaField::unconstrained("constraint_p1", Type::Uint4),
			SchemaField::unconstrained("constraint_p2", Type::Uint4),
			SchemaField::unconstrained("offset", Type::Uint4),
			SchemaField::unconstrained("size", Type::Uint4),
			SchemaField::unconstrained("align", Type::Uint1),
		])
	});
}
