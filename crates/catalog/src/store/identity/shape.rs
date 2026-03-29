// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod identity {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const IDENTITY: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const ENABLED: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("identity", Type::IdentityId),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("enabled", Type::Boolean),
		])
	});
}
