// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod sequence {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("value", ValueType::Uint8)]));
}
