// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod operator_settings {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	pub(crate) const IS_JOIN: usize = 0;

	pub(crate) const CLEANUP_MODE: usize = 1;
	pub(crate) const DURATION_NANOS: usize = 2;

	pub(crate) const LEFT_CLEANUP_MODE: usize = 3;
	pub(crate) const LEFT_DURATION_NANOS: usize = 4;

	pub(crate) const RIGHT_CLEANUP_MODE: usize = 5;
	pub(crate) const RIGHT_DURATION_NANOS: usize = 6;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("is_join", ValueType::Boolean),
			RowShapeField::unconstrained("cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("duration_nanos", ValueType::Uint8),
			RowShapeField::unconstrained("left_cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("left_duration_nanos", ValueType::Uint8),
			RowShapeField::unconstrained("right_cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("right_duration_nanos", ValueType::Uint8),
		])
	});
}
