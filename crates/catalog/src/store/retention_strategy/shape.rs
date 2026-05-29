// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod retention_strategy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const STRATEGY_KEEP_FOREVER: u8 = 0;
	pub(crate) const STRATEGY_KEEP_VERSIONS: u8 = 1;

	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	pub(crate) const STRATEGY_TYPE: usize = 0;
	pub(crate) const CLEANUP_MODE: usize = 1;
	pub(crate) const VALUE: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("strategy_type", ValueType::Uint1),
			RowShapeField::unconstrained("cleanup_mode", ValueType::Uint1),
			RowShapeField::unconstrained("value", ValueType::Uint8),
		])
	});
}
