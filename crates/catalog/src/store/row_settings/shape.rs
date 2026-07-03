// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod row_settings {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	pub(crate) const CLEANUP_MODE: usize = 0;
	pub(crate) const DURATION: usize = 1;
	pub(crate) const PERSISTENT: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("duration", ValueType::Duration),
			RowShapeField::unconstrained("persistent", ValueType::Uint1),
		])
	});
}
