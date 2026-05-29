// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub(crate) mod policy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const TARGET_TYPE: usize = 2;
	pub(crate) const TARGET_NAMESPACE: usize = 3;
	pub(crate) const TARGET_SHAPE: usize = 4;
	pub(crate) const ENABLED: usize = 5;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("target_type", ValueType::Utf8),
			RowShapeField::unconstrained("target_namespace", ValueType::Utf8),
			RowShapeField::unconstrained("target_shape", ValueType::Utf8),
			RowShapeField::unconstrained("enabled", ValueType::Boolean),
		])
	});
}

pub(crate) mod policy_op {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const POLICY_ID: usize = 0;
	pub(crate) const OPERATION: usize = 1;
	pub(crate) const BODY_SOURCE: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("policy_id", ValueType::Uint8),
			RowShapeField::unconstrained("operation", ValueType::Utf8),
			RowShapeField::unconstrained("body_source", ValueType::Utf8),
		])
	});
}
