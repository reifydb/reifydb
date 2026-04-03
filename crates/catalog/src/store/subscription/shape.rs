// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
