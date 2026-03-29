// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod config {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("value", Type::Any)]));
}
