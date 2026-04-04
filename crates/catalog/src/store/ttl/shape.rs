// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod ttl_config {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	// TTL anchor discriminators
	pub(crate) const ANCHOR_CREATED: u8 = 0;
	pub(crate) const ANCHOR_UPDATED: u8 = 1;

	// Cleanup mode discriminators
	pub(crate) const CLEANUP_MODE_DELETE: u8 = 0;
	pub(crate) const CLEANUP_MODE_DROP: u8 = 1;

	// Field indices
	pub(crate) const ANCHOR: usize = 0; // u8: anchor discriminator
	pub(crate) const CLEANUP_MODE: usize = 1; // u8: cleanup mode
	pub(crate) const DURATION_NANOS: usize = 2; // u64: duration in nanoseconds

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("anchor", Type::Uint1),
			RowShapeField::unconstrained("cleanup_mode", Type::Uint1),
			RowShapeField::unconstrained("duration_nanos", Type::Uint8),
		])
	});
}
