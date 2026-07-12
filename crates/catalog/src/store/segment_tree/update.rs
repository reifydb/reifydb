// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{interface::catalog::segment_tree::SegmentTreeMetadata, key::segment_tree::SegmentTreeMetadataKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::segment_tree::shape::segment_tree_metadata};

fn encode_segment_tree_metadata(metadata: &SegmentTreeMetadata) -> EncodedRow {
	let mut row = segment_tree_metadata::SHAPE.allocate();
	segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::ID, metadata.id.0);
	segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::ROW_COUNT, metadata.row_count);
	segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::OLDEST_KEY, metadata.oldest_key);
	segment_tree_metadata::SHAPE.set_u64(&mut row, segment_tree_metadata::NEWEST_KEY, metadata.newest_key);
	segment_tree_metadata::SHAPE.set_u64(
		&mut row,
		segment_tree_metadata::SEQUENCE_COUNTER,
		metadata.sequence_counter,
	);
	row
}

impl CatalogStore {
	pub(crate) fn update_segment_tree_metadata_txn(
		txn: &mut Transaction<'_>,
		metadata: SegmentTreeMetadata,
	) -> Result<()> {
		let row = encode_segment_tree_metadata(&metadata);
		txn.set(&SegmentTreeMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}
}
