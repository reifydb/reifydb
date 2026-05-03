// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod shape_header {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const FIELD_COUNT: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("field_count", Type::Uint2)]));
}

pub(crate) mod shape_field {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const NAME: usize = 0;

	pub(crate) const TYPE: usize = 1;

	pub(crate) const CONSTRAINT_TYPE: usize = 2;

	pub(crate) const CONSTRAINT_P1: usize = 3;

	pub(crate) const CONSTRAINT_P2: usize = 4;

	pub(crate) const OFFSET: usize = 5;

	pub(crate) const SIZE: usize = 6;

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
