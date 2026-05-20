// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod config {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const VALUE: usize = 0;

	pub(crate) static SHAPE: Lazy<RowShape> =
		Lazy::new(|| RowShape::new(vec![RowShapeField::unconstrained("value", Type::Any)]));
}
