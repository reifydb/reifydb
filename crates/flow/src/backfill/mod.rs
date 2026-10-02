// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, num::NonZeroU64};

use arrow_array::RecordBatch;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::object::ObjectId,
		change::{Change, Diff},
	},
	internal_err,
};
use reifydb_value::{
	Result,
	value::system_columns::{SystemColumn, keep_system_columns, require_updated_at},
};

#[cfg(any(test, feature = "testing"))]
pub mod testing;

const LIVE_SYSTEM_COLUMNS: [SystemColumn; 4] =
	[SystemColumn::RowNumbers, SystemColumn::CreatedAt, SystemColumn::UpdatedAt, SystemColumn::Time];

pub trait Scan {
	fn version(&self) -> CommitVersion;

	fn open(&mut self, source: ObjectId, batch_size: NonZeroU64) -> Result<()>;

	fn next(&mut self) -> Result<Option<RecordBatch>>;
}

pub fn backfill<S: Scan>(
	scan: &mut S,
	sources: &BTreeSet<ObjectId>,
	batch_size: NonZeroU64,
	mut consume: impl FnMut(&mut S, Change) -> Result<()>,
) -> Result<()> {
	let version = ChangeVersion::from(scan.version());
	for &source in sources {
		scan.open(source, batch_size)?;
		while let Some(chunk) = scan.next()? {
			if chunk.num_rows() == 0 {
				continue;
			}
			consume(scan, snapshot_change(source, version, &chunk)?)?;
		}
	}
	Ok(())
}

fn snapshot_change(source: ObjectId, version: ChangeVersion, chunk: &RecordBatch) -> Result<Change> {
	let Some(changed_at) = require_updated_at(chunk)?.iter().copied().max() else {
		return internal_err!(
			"snapshot chunk of {} rows from {:?} has no updated_at stamps",
			chunk.num_rows(),
			source
		);
	};
	let post = keep_system_columns(chunk, &LIVE_SYSTEM_COLUMNS)?;
	Ok(Change::from_object(source, version, vec![Diff::insert(post)], changed_at))
}
