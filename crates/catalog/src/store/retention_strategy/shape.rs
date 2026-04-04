// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod retention_strategy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	// Retention strategy discriminators
	pub(crate) const STRATEGY_KEEP_FOREVER: u8 = 0;
	pub(crate) const STRATEGY_KEEP_VERSIONS: u8 = 1;

	// Cleanup mode discriminators
	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	// Field indices
	pub(crate) const STRATEGY_TYPE: usize = 0; // u8: strategy discriminator
	pub(crate) const CLEANUP_MODE: usize = 1; // u8: cleanup mode
	pub(crate) const VALUE: usize = 2; // u64: numeric value (version count, duration secs, or commit version)

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("strategy_type", Type::Uint1),
			RowShapeField::unconstrained("cleanup_mode", Type::Uint1),
			RowShapeField::unconstrained("value", Type::Uint8),
		])
	});
}
