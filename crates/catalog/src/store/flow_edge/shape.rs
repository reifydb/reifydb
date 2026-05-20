// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod flow_edge {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const SOURCE: usize = 2;
	pub(crate) const TARGET: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("flow", Type::Uint8),
			RowShapeField::unconstrained("source", Type::Uint8),
			RowShapeField::unconstrained("target", Type::Uint8),
		])
	});
}

pub(crate) mod flow_edge_by_flow {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const FLOW: usize = 0;
	pub(crate) const ID: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("flow", Type::Uint8),
			RowShapeField::unconstrained("id", Type::Uint8),
		])
	});
}
