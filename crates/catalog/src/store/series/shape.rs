// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use once_cell::sync::Lazy;
use reifydb_core::encoded::shape::{RowShape, RowShapeField};
use reifydb_type::value::r#type::Type;

pub(crate) mod series {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const TAG: usize = 3;
	pub(crate) const KEY_COLUMN: usize = 4;
	pub(crate) const KEY_KIND: usize = 5;
	pub(crate) const PRECISION: usize = 6;
	pub(crate) const PRIMARY_KEY: usize = 7;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("namespace", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
			RowShapeField::unconstrained("tag", Type::Uint8),
			RowShapeField::unconstrained("key_column", Type::Utf8),
			RowShapeField::unconstrained("key_kind", Type::Uint1),
			RowShapeField::unconstrained("precision", Type::Uint1),
			RowShapeField::unconstrained("primary_key", Type::Uint8),
		])
	});
}

pub(crate) mod series_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("name", Type::Utf8),
		])
	});
}

pub(crate) mod series_metadata {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const ROW_COUNT: usize = 1;
	pub(crate) const OLDEST_KEY: usize = 2;
	pub(crate) const NEWEST_KEY: usize = 3;
	pub(crate) const SEQUENCE_COUNTER: usize = 4;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", Type::Uint8),
			RowShapeField::unconstrained("row_count", Type::Uint8),
			RowShapeField::unconstrained("oldest_key", Type::Uint8),
			RowShapeField::unconstrained("newest_key", Type::Uint8),
			RowShapeField::unconstrained("sequence_counter", Type::Uint8),
		])
	});
}
