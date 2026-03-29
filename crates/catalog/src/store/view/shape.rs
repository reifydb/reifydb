// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod view {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KIND: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;
	pub(crate) const STORAGE_KIND: usize = 5;
	pub(crate) const UNDERLYING_PRIMITIVE_ID: usize = 6;
	pub(crate) const CAPACITY: usize = 7;
	pub(crate) const PROPAGATE_EVICTIONS: usize = 8;
	pub(crate) const KEY_COLUMN: usize = 9;
	pub(crate) const KEY_KIND: usize = 10;
	pub(crate) const PRECISION: usize = 11;
	pub(crate) const TAG_ID: usize = 12;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("kind", Type::Uint1),
			RowShapeField::unconstrained("primary_key", Type::Uint8),
			RowShapeField::unconstrained("storage_kind", Type::Uint1),
			RowShapeField::unconstrained("underlying_object_id", Type::Uint8),
			RowShapeField::unconstrained("capacity", Type::Uint8),
			RowShapeField::unconstrained("propagate_evictions", Type::Uint1),
			RowShapeField::unconstrained("key_column", Type::Utf8),
			RowShapeField::unconstrained("key_kind", Type::Uint1),
			RowShapeField::unconstrained("precision", Type::Uint1),
			RowShapeField::unconstrained("tag_id", Type::Uint8),
		])
	});
}

pub(crate) mod view_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
		])
	});
}
