// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod flow_node {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const TYPE: usize = 2;
	pub(crate) const DATA: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("flow", Type::Uint8),
			RowShapeField::unconstrained("type", Type::Uint1),
			RowShapeField::unconstrained("data", Type::Blob),
		])
	});
}

pub(crate) mod flow_node_by_flow {
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
