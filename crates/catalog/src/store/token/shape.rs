// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod token {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const TOKEN: usize = 1;
	pub(crate) const IDENTITY: usize = 2;
	pub(crate) const EXPIRES_AT: usize = 3;
	pub(crate) const CREATED_AT: usize = 4;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("token", ValueType::Utf8),
			RowShapeField::unconstrained("identity", ValueType::IdentityId),
			RowShapeField::unconstrained("expires_at", ValueType::DateTime),
			RowShapeField::unconstrained("created_at", ValueType::DateTime),
		])
	});
}
