// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod subscription {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub const ID: usize = 0;
	pub const ACKNOWLEDGED_VERSION: usize = 1;
	pub const PRIMARY_KEY: usize = 2;

	pub static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("acknowledged_version", ValueType::Uint8),
			RowShapeField::unconstrained("primary_key", ValueType::Uint8),
		])
	});
}

pub mod subscription_column {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub const ID: usize = 0;
	pub const NAME: usize = 1;
	pub const TYPE: usize = 2;

	pub static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("type", ValueType::Uint1),
		])
	});
}
