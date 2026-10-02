// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{
			object::ObjectId,
			series::{Series, SeriesPartitionMetadata},
		},
		change::{Change, ChangeOrigin, Diff},
	},
	key::any::TaggedKey,
};
use reifydb_transaction::{
	interceptor::{WithInterceptors, series_row::SeriesRowInterceptor},
	transaction::Transaction,
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use smallvec::smallvec;

use crate::Result;

pub(crate) fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: RecordBatch) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

pub(crate) fn remove_series_rows(
	txn: &mut Transaction<'_>,
	series: &Series,
	ids: &[RowNumber],
	removals: &[(TaggedKey, EncodedBytes, bool)],
) -> Result<()> {
	assert_eq!(ids.len(), removals.len(), "ids/removals length mismatch");
	if ids.is_empty() {
		return Ok(());
	}
	if !txn.series_row_pre_delete_interceptors().is_empty() {
		SeriesRowInterceptor::pre_delete(txn, series, ids)?;
	}
	for (key, pre_for_cdc, was_committed) in removals {
		if *was_committed {
			txn.mark_preexisting(key)?;
		}
		txn.remove_with_pre(key, pre_for_cdc.clone())?;
	}
	if !txn.series_row_post_delete_interceptors().is_empty() {
		let pre_rows: Vec<EncodedBytes> =
			removals.iter().map(|(_, pre_for_cdc, _)| pre_for_cdc.clone()).collect();
		SeriesRowInterceptor::post_delete(txn, series, &pre_rows)?;
	}
	Ok(())
}

pub struct SeriesDeleteTally {
	pub count: u64,
	pub min_key: u64,
	pub max_key: u64,
}

impl SeriesDeleteTally {
	pub fn new() -> Self {
		Self {
			count: 0,
			min_key: u64::MAX,
			max_key: 0,
		}
	}

	pub fn record(&mut self, key: u64) {
		self.count += 1;
		self.min_key = self.min_key.min(key);
		self.max_key = self.max_key.max(key);
	}
}

impl Default for SeriesDeleteTally {
	fn default() -> Self {
		Self::new()
	}
}

pub fn apply_series_metadata_after_delete(metadata: &mut SeriesPartitionMetadata, tally: &SeriesDeleteTally) {
	metadata.row_count = metadata.row_count.saturating_sub(tally.count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}
	if tally.count > 0 {
		metadata.dirty_from_key = metadata.dirty_from_key.min(tally.min_key);
		metadata.dirty_to_key = metadata.dirty_to_key.max(tally.max_key.saturating_add(1));
	}
}
