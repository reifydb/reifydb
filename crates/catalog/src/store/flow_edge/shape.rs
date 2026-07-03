// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod flow_edge {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const SOURCE: usize = 2;
	pub(crate) const TARGET: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("flow", ValueType::Uint8),
			RowShapeField::unconstrained("source", ValueType::Uint8),
			RowShapeField::unconstrained("target", ValueType::Uint8),
		])
	});
}

pub(crate) mod flow_edge_by_flow {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const FLOW: usize = 0;
	pub(crate) const ID: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("flow", ValueType::Uint8),
			RowShapeField::unconstrained("id", ValueType::Uint8),
		])
	});
}
