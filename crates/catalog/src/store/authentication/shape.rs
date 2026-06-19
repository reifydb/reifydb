// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod authentication {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const IDENTITY: usize = 1;
	pub(crate) const METHOD: usize = 2;
	pub(crate) const PROPERTIES: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("identity", ValueType::IdentityId),
			RowShapeField::unconstrained("method", ValueType::Utf8),
			RowShapeField::unconstrained("properties", ValueType::Utf8),
		])
	});
}
