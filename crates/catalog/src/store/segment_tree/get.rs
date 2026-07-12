// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SegmentTreeId, segment_tree::SegmentTree},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_segment_tree(rx: &mut Transaction<'_>, segment_tree: SegmentTreeId) -> Result<SegmentTree> {
		Self::find_segment_tree(rx, segment_tree)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"SegmentTree with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				segment_tree
			)))
		})
	}
}
