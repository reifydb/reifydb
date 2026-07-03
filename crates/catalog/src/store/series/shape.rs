// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use once_cell::sync::Lazy;
use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
use reifydb_value::value::value_type::ValueType;

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
	pub(crate) const UNDERLYING: usize = 8;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("tag", ValueType::Uint8),
			RowShapeField::unconstrained("key_column", ValueType::Utf8),
			RowShapeField::unconstrained("key_kind", ValueType::Uint1),
			RowShapeField::unconstrained("precision", ValueType::Uint1),
			RowShapeField::unconstrained("primary_key", ValueType::Uint8),
			RowShapeField::unconstrained("underlying", ValueType::Uint1),
		])
	});
}

pub(crate) mod series_namespace {
	use super::*;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
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
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("row_count", ValueType::Uint8),
			RowShapeField::unconstrained("oldest_key", ValueType::Uint8),
			RowShapeField::unconstrained("newest_key", ValueType::Uint8),
			RowShapeField::unconstrained("sequence_counter", ValueType::Uint8),
		])
	});
}
