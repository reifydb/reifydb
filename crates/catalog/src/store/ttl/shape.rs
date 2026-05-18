// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod ttl_config {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ANCHOR_CREATED: u8 = 0;
	pub(crate) const ANCHOR_UPDATED: u8 = 1;

	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	pub(crate) const ANCHOR: usize = 0;
	pub(crate) const CLEANUP_MODE: usize = 1;
	pub(crate) const DURATION_NANOS: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("anchor", Type::Uint1),
			RowShapeField::unconstrained("cleanup_mode", Type::Uint1),
			RowShapeField::unconstrained("duration_nanos", Type::Uint8),
		])
	});
}
