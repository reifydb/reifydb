// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_value::value::datetime::DateTime;
use tracing::instrument;

use crate::{
	error::CdcError,
	storage::{CdcStorage, CdcStorageResult, Cutoff, DropBeforeResult},
	store::CdcStore,
};

impl CdcStorage for CdcStore {
	#[instrument(name = "store::cdc::write", level = "debug", skip(self, cdc), fields(version = cdc.version.0, change_count = cdc.changes.len()))]
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		if self.commit.append(Arc::new(cdc.clone())) {
			return Ok(());
		}
		Err(CdcError::DuplicateVersion(cdc.version))
	}

	#[instrument(name = "store::cdc::read", level = "trace", skip(self), fields(version = version.0))]
	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		if let Some(cdc) = self.commit.get(version) {
			return Ok(Some((*cdc).clone()));
		}
		let Some(block) = self.block_for(version)? else {
			return Ok(None);
		};
		Ok(block.entries
			.binary_search_by(|cdc| cdc.version.cmp(&version))
			.ok()
			.map(|index| (*block.entries[index]).clone()))
	}

	#[instrument(name = "store::cdc::read_range", level = "trace", skip(self, start, end), fields(batch_size = batch_size))]
	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		self.walk_range(start, end, batch_size)
	}

	#[instrument(name = "store::cdc::count", level = "trace", skip(self), fields(version = version.0))]
	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		Ok(self.read(version)?.map(|cdc| cdc.changes.len()).unwrap_or(0))
	}

	#[instrument(name = "store::cdc::min_version", level = "trace", skip(self))]
	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		if let Some(version) = self.persistent.min_version()? {
			return Ok(Some(version));
		}
		Ok(self.commit.floor())
	}

	#[instrument(name = "store::cdc::max_version", level = "trace", skip(self))]
	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		if let Some(version) = self.commit.head() {
			return Ok(Some(version));
		}
		Ok(self.persistent.max_version()?)
	}

	#[instrument(name = "store::cdc::drop_before", level = "debug", skip(self), fields(cutoff = ?cutoff, limit = limit))]
	fn drop_before(&self, cutoff: Cutoff, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		if let Some(read) = self.read.as_ref() {
			match cutoff {
				Cutoff::Version(version) => read.invalidate_below(version),
				Cutoff::Unbounded => read.clear(),
			}
		}
		let outcome = self.persistent.drop_blocks_below(cutoff, limit)?;
		let floor = self.persistent.truncated_before();
		if let Some(read) = self.read.as_ref() {
			match cutoff {
				Cutoff::Version(_) => read.invalidate_below(floor),
				Cutoff::Unbounded => read.clear(),
			}
		}
		if floor.0 > 0 {
			self.commit.seal_floor(CommitVersion(floor.0 - 1));
		}
		Ok(DropBeforeResult {
			count: outcome.count,
			entries: outcome.entries,
			more_remaining: outcome.more_remaining,
		})
	}

	#[instrument(name = "store::cdc::truncated_before", level = "trace", skip(self))]
	fn truncated_before(&self) -> CdcStorageResult<CommitVersion> {
		Ok(self.persistent.truncated_before())
	}

	#[instrument(name = "store::cdc::find_ttl_cutoff", level = "debug", skip(self, cutoff))]
	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<Cutoff>> {
		Ok(self.persistent.find_ttl_cutoff(cutoff)?)
	}
}
