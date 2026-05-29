// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const PARENT_ID: usize = 2;
	pub(crate) const GRPC: usize = 3;
	pub(crate) const LOCAL_NAME: usize = 4;
	pub(crate) const TOKEN: usize = 5;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("parent_id", ValueType::Uint8),
			RowShapeField::unconstrained("grpc", ValueType::Utf8),
			RowShapeField::unconstrained("local_name", ValueType::Utf8),
			RowShapeField::unconstrained("token", ValueType::Utf8),
		])
	});
}
