// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub(crate) mod relationship {
	use once_cell::sync::Lazy;
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE_ID: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const SOURCE_TABLE_ID: usize = 3;
	pub(crate) const SOURCE_COLUMN_ID: usize = 4;
	pub(crate) const TARGET_TABLE_ID: usize = 5;
	pub(crate) const TARGET_COLUMN_ID: usize = 6;
	pub(crate) const JUNCTION_TABLE_ID: usize = 7;
	pub(crate) const JUNCTION_SOURCE_COLUMN_ID: usize = 8;
	pub(crate) const JUNCTION_TARGET_COLUMN_ID: usize = 9;
	pub(crate) const CARDINALITY: usize = 10;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace_id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("source_table_id", ValueType::Uint8),
			RowShapeField::unconstrained("source_column_id", ValueType::Uint8),
			RowShapeField::unconstrained("target_table_id", ValueType::Uint8),
			RowShapeField::unconstrained("target_column_id", ValueType::Uint8),
			RowShapeField::unconstrained("junction_table_id", ValueType::Uint8),
			RowShapeField::unconstrained("junction_source_column_id", ValueType::Uint8),
			RowShapeField::unconstrained("junction_target_column_id", ValueType::Uint8),
			RowShapeField::unconstrained("cardinality", ValueType::Uint1),
		])
	});
}
