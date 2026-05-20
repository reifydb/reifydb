// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const ACKNOWLEDGED_VERSION: usize = 1;
	pub const PRIMARY_KEY: usize = 2;

	pub static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("acknowledged_version", Type::Uint8),
			RowShapeField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub mod subscription_column {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub const ID: usize = 0;
	pub const NAME: usize = 1;
	pub const TYPE: usize = 2;

	pub static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("type", Type::Uint1),
		])
	});
}
