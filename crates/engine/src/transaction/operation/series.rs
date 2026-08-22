// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			object::ObjectId,
			series::{Series, SeriesMetadata},
		},
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::columns::Columns,
};
use reifydb_transaction::{interceptor::series_row::SeriesRowInterceptor, transaction::Transaction};
use reifydb_value::value::datetime::DateTime;
use smallvec::smallvec;

use crate::Result;

pub(crate) fn emit_series_remove_change(txn: &mut Transaction<'_>, series: &Series, pre: Columns) {
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::series(series.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(pre)],
		changed_at: DateTime::default(),
	});
}

pub fn remove_series_row(
	txn: &mut Transaction<'_>,
	series: &Series,
	key: &EncodedKey,
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

pub fn apply_series_metadata_after_delete(metadata: &mut SeriesMetadata, deleted_count: u64) {
	metadata.row_count = metadata.row_count.saturating_sub(deleted_count);
	if metadata.row_count == 0 {
		metadata.oldest_key = 0;
		metadata.newest_key = 0;
	}
}
