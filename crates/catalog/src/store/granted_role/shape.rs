// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod granted_role {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const IDENTITY: usize = 0;
	pub(crate) const ROLE_ID: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("identity", ValueType::IdentityId),
			RowShapeField::unconstrained("role_id", ValueType::Uint8),
		])
	});
}
