// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! RowShape definitions for shape data storage.

pub(crate) mod shape_header {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	/// Field count index
	pub(crate) const FIELD_COUNT: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("field_count", Type::Uint2)]));
}

pub(crate) mod shape_field {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
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

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("base_type", Type::Uint1),
			RowShapeField::unconstrained("constraint_type", Type::Uint1),
			RowShapeField::unconstrained("constraint_p1", Type::Uint4),
			RowShapeField::unconstrained("constraint_p2", Type::Uint4),
			RowShapeField::unconstrained("offset", Type::Uint4),
			RowShapeField::unconstrained("size", Type::Uint4),
			RowShapeField::unconstrained("align", Type::Uint1),
		])
	});
}
