// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod sink {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const SOURCE_NAMESPACE: usize = 3;
	pub(crate) const SOURCE_NAME: usize = 4;
	pub(crate) const CONNECTOR: usize = 5;
	pub(crate) const CONFIG: usize = 6;
	pub(crate) const STATUS: usize = 7;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("source_namespace", Type::Uint8),
			RowShapeField::unconstrained("source_name", Type::Utf8),
			RowShapeField::unconstrained("connector", Type::Utf8),
			RowShapeField::unconstrained("config", Type::Utf8),
			RowShapeField::unconstrained("status", Type::Uint1),
		])
	});
}

pub(crate) mod sink_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
		])
	});
}
