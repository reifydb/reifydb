// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod column_property {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const COLUMN: usize = 1;
	pub(crate) const POLICY: usize = 2;
	pub(crate) const VALUE: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("column", ValueType::Uint8),
			RowShapeField::unconstrained("policy", ValueType::Uint1),
			RowShapeField::unconstrained("value", ValueType::Uint1),
		])
	});
}
