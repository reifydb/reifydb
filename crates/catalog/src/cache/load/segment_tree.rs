// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, SegmentTreeId},
			key::{KeySpec, PrimaryKey},
			segment_tree::{SegmentTree, SegmentTreeAggregate},
			shape::ShapeId,
		},
		store::MultiVersionRow,
	},
	key::segment_tree::SegmentTreeKey as SegmentTreeStorageKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{CatalogStore, Result, store::segment_tree::shape::segment_tree};

pub(crate) fn load_segment_tree(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = SegmentTreeStorageKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut segment_tree_list = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_segment_tree_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let segment_tree = convert_segment_tree(multi, primary_key);

		if let Some(id) = pk_id {
			catalog.set_primary_key_shape(ShapeId::SegmentTree(segment_tree.id), id);
		}
		segment_tree_list.push((segment_tree, version));
	}
	drop(stream);

	for (mut segment_tree, version) in segment_tree_list {
		segment_tree.columns = CatalogStore::list_columns(rx, segment_tree.id)?;
		catalog.set_segment_tree(segment_tree.id, version, Some(segment_tree));
	}

	Ok(())
}

fn convert_segment_tree(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> SegmentTree {
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

	SegmentTree {
		id,
		namespace,
		name,
		columns: vec![],
		key,
		aggregates,
		primary_key,
		partition_by,
		underlying,
	}
}

fn get_segment_tree_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = segment_tree::SHAPE.get_u64(&multi.row, segment_tree::PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
