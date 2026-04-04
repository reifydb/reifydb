// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod policy {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const TARGET_TYPE: usize = 2;
	pub(crate) const TARGET_NAMESPACE: usize = 3;
	pub(crate) const TARGET_SHAPE: usize = 4;
	pub(crate) const ENABLED: usize = 5;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("target_type", Type::Utf8),
			RowShapeField::unconstrained("target_namespace", Type::Utf8),
			RowShapeField::unconstrained("target_shape", Type::Utf8),
			RowShapeField::unconstrained("enabled", Type::Boolean),
		])
	});
}

pub(crate) mod policy_op {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const POLICY_ID: usize = 0;
	pub(crate) const OPERATION: usize = 1;
	pub(crate) const BODY_SOURCE: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("policy_id", Type::Uint8),
			RowShapeField::unconstrained("operation", Type::Utf8),
			RowShapeField::unconstrained("body_source", Type::Utf8),
		])
	});
}
