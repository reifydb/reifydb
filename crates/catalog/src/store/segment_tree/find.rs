// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, SegmentTreeId},
		key::KeySpec,
		segment_tree::{SegmentTree, SegmentTreeAggregate, SegmentTreeMetadata},
	},
	key::{
		namespace_segment_tree::NamespaceSegmentTreeKey,
		segment_tree::{SegmentTreeKey, SegmentTreeMetadataKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::segment_tree::shape::{segment_tree, segment_tree_metadata, segment_tree_namespace},
};

impl CatalogStore {
	pub(crate) fn find_segment_tree(
		rx: &mut Transaction<'_>,
		segment_tree_id: SegmentTreeId,
	) -> Result<Option<SegmentTree>> {
		let Some(multi) = rx.get(&SegmentTreeKey::encoded(segment_tree_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SegmentTreeId(segment_tree::SHAPE.get_u64(&row, segment_tree::ID));
		let namespace = NamespaceId(segment_tree::SHAPE.get_u64(&row, segment_tree::NAMESPACE));
		let name = segment_tree::SHAPE.get_utf8(&row, segment_tree::NAME).to_string();
		let key_column = segment_tree::SHAPE.get_utf8(&row, segment_tree::KEY_COLUMN).to_string();
		let key_kind_raw = segment_tree::SHAPE.get_u8(&row, segment_tree::KEY_KIND);
		let precision_raw = segment_tree::SHAPE.get_u8(&row, segment_tree::PRECISION);
		let key = KeySpec::decode(key_kind_raw, precision_raw, key_column);
		let partition_by_str = segment_tree::SHAPE.get_utf8(&row, segment_tree::PARTITION_BY);
		let partition_by = if partition_by_str.is_empty() {
			vec![]
		} else {
			partition_by_str.split(',').map(|s| s.to_string()).collect()
		};
		let underlying = segment_tree::SHAPE.get_u8(&row, segment_tree::UNDERLYING) != 0;
		let aggregates_blob = segment_tree::SHAPE.get_blob(&row, segment_tree::AGGREGATES);
		let aggregates: Vec<SegmentTreeAggregate> = from_bytes(aggregates_blob.as_bytes())
			.expect("SegmentTreeAggregate vec must deserialize with postcard");

		Ok(Some(SegmentTree {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			key,
			aggregates,
			primary_key: Self::find_primary_key(rx, id)?,
			partition_by,
			underlying,
		}))
	}

	pub(crate) fn find_segment_tree_metadata(
		rx: &mut Transaction<'_>,
		segment_tree_id: SegmentTreeId,
	) -> Result<Option<SegmentTreeMetadata>> {
		let Some(multi) = rx.get(&SegmentTreeMetadataKey::encoded(segment_tree_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = SegmentTreeId(segment_tree_metadata::SHAPE.get_u64(&row, segment_tree_metadata::ID));
		let row_count = segment_tree_metadata::SHAPE.get_u64(&row, segment_tree_metadata::ROW_COUNT);
		let oldest_key = segment_tree_metadata::SHAPE.get_u64(&row, segment_tree_metadata::OLDEST_KEY);
		let newest_key = segment_tree_metadata::SHAPE.get_u64(&row, segment_tree_metadata::NEWEST_KEY);
		let sequence_counter =
			segment_tree_metadata::SHAPE.get_u64(&row, segment_tree_metadata::SEQUENCE_COUNTER);

		Ok(Some(SegmentTreeMetadata {
			id,
			row_count,
			oldest_key,
			newest_key,
			sequence_counter,
		}))
	}

	pub(crate) fn find_segment_tree_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<SegmentTree>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceSegmentTreeKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_segment_tree = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let row = &multi.row;
			let segment_tree_name =
				segment_tree_namespace::SHAPE.get_utf8(row, segment_tree_namespace::NAME);
			if name == segment_tree_name {
				found_segment_tree = Some(SegmentTreeId(
					segment_tree_namespace::SHAPE.get_u64(row, segment_tree_namespace::ID),
				));
				break;
			}
		}

		drop(stream);

		let Some(segment_tree_id) = found_segment_tree else {
			return Ok(None);
		};

		Ok(Some(Self::get_segment_tree(rx, segment_tree_id)?))
	}
}
