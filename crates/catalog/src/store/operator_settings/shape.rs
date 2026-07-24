// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod operator_settings {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const IS_JOIN: usize = 0;

	pub(crate) const DURATION: usize = 1;

	pub(crate) const LEFT_DURATION: usize = 2;

	pub(crate) const RIGHT_DURATION: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("is_join", ValueType::Boolean),
			RowShapeField::unconstrained("duration", ValueType::Duration),
			RowShapeField::unconstrained("left_duration", ValueType::Duration),
			RowShapeField::unconstrained("right_duration", ValueType::Duration),
		])
	});
}
