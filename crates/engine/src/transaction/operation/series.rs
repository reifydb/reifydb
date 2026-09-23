// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	value::column::columns::Columns,
};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_value::value::datetime::DateTime;
use smallvec::smallvec;

use crate::Result;

pub(crate) fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: Columns) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

pub fn remove_series_row(
	txn: &mut Transaction<'_>,
	series: &Series,
	key: &TaggedKey,
	pre_for_cdc: EncodedBytes,
	was_committed: bool,
	pre: Option<Columns>,
) -> Result<()> {
	if let Some(pre) = pre {
		emit_series_remove_change(txn, series, pre);
	}
	SeriesRowInterceptor::pre_delete(txn, series)?;
	if was_committed {
		txn.mark_preexisting(key)?;
	}
	txn.remove_with_pre(key, pre_for_cdc.clone())?;
	let pre_rows = [pre_for_cdc];
	SeriesRowInterceptor::post_delete(txn, series, &pre_rows)?;
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
