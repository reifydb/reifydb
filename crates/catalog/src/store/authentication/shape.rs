// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod authentication {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const IDENTITY: usize = 1;
	pub(crate) const METHOD: usize = 2;
	pub(crate) const PROPERTIES: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("identity", Type::IdentityId),
			RowShapeField::unconstrained("method", Type::Utf8),
			RowShapeField::unconstrained("properties", Type::Utf8),
		])
	});
}
