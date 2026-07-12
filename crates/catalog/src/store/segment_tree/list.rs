// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SegmentTreeId, segment_tree::SegmentTree},
	key::{Key, segment_tree::SegmentTreeKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_segment_tree_all(rx: &mut Transaction<'_>) -> Result<Vec<SegmentTree>> {
		let mut result = Vec::new();

		let mut segment_tree_data: Vec<SegmentTreeId> = Vec::new();
		{
			let stream = rx.range(SegmentTreeKey::full_scan(), RangeScope::All, 1024)?;

			for entry in stream {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key)
					&& let Key::SegmentTree(segment_tree_key) = key
				{
					segment_tree_data.push(segment_tree_key.segment_tree);
				}
			}
		}

		for segment_tree_id in segment_tree_data {
			result.push(Self::get_segment_tree(rx, segment_tree_id)?);
		}

		Ok(result)
	}
}
