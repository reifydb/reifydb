// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::{byte_size::ByteSize, count::Count};
use tracing::instrument;

use crate::{
	tier::{
		commit::{CdcCommitBufferTier, batch::FlushBatch},
		persistent::CdcPersistentTier,
		read::CdcReadBufferTier,
	},
	types::{Block, BlockId, BlockSummary},
};

#[instrument(name = "store::cdc::flush::seal", level = "trace", skip_all, fields(entry_count = batch.entries.len()))]
fn seal(batch: &FlushBatch) -> Option<Block> {
	let first = batch.entries.first()?;
	let last = batch.entries.last()?;
	let min_timestamp = batch.entries.iter().map(|cdc| cdc.timestamp).min()?;
	let max_timestamp = batch.entries.iter().map(|cdc| cdc.timestamp).max()?;
	Some(Block {
		summary: BlockSummary {
			id: BlockId(last.version),
			min_version: first.version,
			max_version: last.version,
			min_timestamp,
			max_timestamp,
			count: Count::new(batch.entries.len() as u64),
			stored_bytes: ByteSize::ZERO,
		},
		entries: batch.entries.clone(),
	})
}

#[instrument(name = "store::cdc::flush::flush_now", level = "debug", skip_all)]
pub fn flush_now(buffer: &CdcCommitBufferTier, storage: &CdcPersistentTier, read: Option<&CdcReadBufferTier>) {
	flush_with(buffer, storage, read, &mut || {});
}

#[instrument(name = "store::cdc::flush::flush_with", level = "debug", skip_all)]
pub fn flush_with(
	buffer: &CdcCommitBufferTier,
	storage: &CdcPersistentTier,
	read: Option<&CdcReadBufferTier>,
	staged: &mut dyn FnMut(),
) {
	let _flushing = buffer.flush_guard();
	while let Some(batch) = buffer.take_for_flush() {
		staged();
		if let Some(block) = seal(&batch) {
			if let Err(err) = storage.append_block(&block) {
				panic!("cdc flush failed to append block {:?}: {err:?}", block.id());
			}
			if let Some(read) = read {
				read.insert(Arc::new(block));
			}
		}
		buffer.complete_flush();
	}
}
