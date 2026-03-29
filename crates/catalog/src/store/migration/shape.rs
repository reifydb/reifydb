// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_type::value::r#type::Type;

pub(crate) mod migration {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;
	pub(crate) const BODY: usize = 2;
	pub(crate) const ROLLBACK_BODY: usize = 3;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("body", Type::Utf8),
			RowShapeField::unconstrained("rollback_body", Type::Utf8),
		])
	});
}

pub(crate) mod migration_event {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const MIGRATION_ID: usize = 1;
	pub(crate) const ACTION: usize = 2;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("migration_id", Type::Uint8),
			RowShapeField::unconstrained("action", Type::Uint1),
		])
	});
}
