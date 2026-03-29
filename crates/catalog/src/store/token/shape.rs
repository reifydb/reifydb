// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod token {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const TOKEN: usize = 1;
	pub(crate) const IDENTITY: usize = 2;
	pub(crate) const EXPIRES_AT: usize = 3;
	pub(crate) const CREATED_AT: usize = 4;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("token", Type::Utf8),
			RowShapeField::unconstrained("identity", Type::IdentityId),
			RowShapeField::unconstrained("expires_at", Type::DateTime),
			RowShapeField::unconstrained("created_at", Type::DateTime),
		])
	});
}
