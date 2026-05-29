// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod row_settings {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ANCHOR_CREATED: u8 = 0;
	pub(crate) const ANCHOR_UPDATED: u8 = 1;

	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	pub(crate) const ANCHOR: usize = 0;
	pub(crate) const CLEANUP_MODE: usize = 1;
	pub(crate) const DURATION_NANOS: usize = 2;
	pub(crate) const PERSISTENT: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("anchor", ValueType::Uint1),
			RowShapeField::unconstrained("cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("duration_nanos", ValueType::Uint8),
			RowShapeField::unconstrained("persistent", ValueType::Uint1),
		])
	});
}
