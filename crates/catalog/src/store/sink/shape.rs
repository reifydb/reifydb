// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod sink {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

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
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("source_namespace", ValueType::Uint8),
			RowShapeField::unconstrained("source_name", ValueType::Utf8),
			RowShapeField::unconstrained("connector", ValueType::Utf8),
			RowShapeField::unconstrained("config", ValueType::Utf8),
			RowShapeField::unconstrained("status", ValueType::Uint1),
		])
	});
}

pub(crate) mod sink_namespace {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
		])
	});
}
