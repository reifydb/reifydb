// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::{EncodedCatalogRow, EncodedCatalogRowBuilder};
use reifydb_core::{
	interface::catalog::{column::ColumnIndex, id::QueueId, view::ViewSortKey},
	sort::SortDirection,
};
use reifydb_macro::catalog_shape;

catalog_shape! {
	pub(crate) view {
		id: u64,
		namespace: u64,
		name: utf8,
		kind: u8,
		primary_key: u64,
		storage_kind: u8,
		capacity: u64,
		key_column: utf8,
		key_kind: u8,
		precision: u8,
		tag_id: u64,
		partition_by: utf8,
		sort: utf8,
	}

	pub(crate) view_namespace {
		id: u64,
		name: utf8,
	}
}

pub(crate) fn set_queue_id(row: &mut EncodedCatalogRowBuilder, queue: QueueId) {
	view::set_capacity(row, queue.0);
}

pub(crate) fn get_queue_id(bytes: &EncodedCatalogRow) -> QueueId {
	QueueId(view::get_capacity(bytes))
}

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
