// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(crate) mod view {
	use once_cell::sync::Lazy;
	use reifydb_core::{
		encoded::shape::{RowShape, RowShapeField},
		interface::catalog::{column::ColumnIndex, view::ViewSortKey},
		sort::SortDirection,
	};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAMESPACE: usize = 1;
	pub(crate) const NAME: usize = 2;
	pub(crate) const KIND: usize = 3;
	pub(crate) const PRIMARY_KEY: usize = 4;
	pub(crate) const STORAGE_KIND: usize = 5;
	pub(crate) const UNDERLYING_SHAPE_ID: usize = 6;
	pub(crate) const CAPACITY: usize = 7;
	pub(crate) const PROPAGATE_EVICTIONS: usize = 8;
	pub(crate) const KEY_COLUMN: usize = 9;
	pub(crate) const KEY_KIND: usize = 10;
	pub(crate) const PRECISION: usize = 11;
	pub(crate) const TAG_ID: usize = 12;
	pub(crate) const SORT: usize = 13;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("namespace", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
			RowShapeField::unconstrained("kind", ValueType::Uint1),
			RowShapeField::unconstrained("primary_key", ValueType::Uint8),
			RowShapeField::unconstrained("storage_kind", ValueType::Uint1),
			RowShapeField::unconstrained("underlying_shape_id", ValueType::Uint8),
			RowShapeField::unconstrained("capacity", ValueType::Uint8),
			RowShapeField::unconstrained("propagate_evictions", ValueType::Uint1),
			RowShapeField::unconstrained("key_column", ValueType::Utf8),
			RowShapeField::unconstrained("key_kind", ValueType::Uint1),
			RowShapeField::unconstrained("precision", ValueType::Uint1),
			RowShapeField::unconstrained("tag_id", ValueType::Uint8),
			RowShapeField::unconstrained("sort", ValueType::Utf8),
		])
	});

	pub(crate) fn encode_view_sort(sort: &[ViewSortKey]) -> String {
		sort.iter()
			.map(|key| {
				let dir = match key.direction {
					SortDirection::Asc => 'a',
					SortDirection::Desc => 'd',
				};
				format!("{}:{}", key.column.0, dir)
			})
			.collect::<Vec<_>>()
			.join(",")
	}

	pub(crate) fn parse_view_sort(encoded: &str) -> Vec<ViewSortKey> {
		if encoded.is_empty() {
			return Vec::new();
		}
		encoded.split(',')
			.filter_map(|part| {
				let (idx, dir) = part.split_once(':')?;
				let column = ColumnIndex(idx.parse::<u8>().ok()?);
				let direction = match dir {
					"a" => SortDirection::Asc,
					"d" => SortDirection::Desc,
					_ => return None,
				};
				Some(ViewSortKey {
					column,
					direction,
				})
			})
			.collect()
	}
}

pub(crate) mod view_namespace {
	use once_cell::sync::Lazy;
	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_value::value::value_type::ValueType;

	pub(crate) const ID: usize = 0;
	pub(crate) const NAME: usize = 1;

	pub(crate) static SHAPE: Lazy<RowShape> = Lazy::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("name", ValueType::Utf8),
		])
	});
}
