// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	error::Result,
	persistent::{
		Apply,
		memory::{MemoryPersistent, row_bytes},
	},
	types::{Applied, DropMarker, FlushBatch, StagedWrite},
};

impl Apply for MemoryPersistent {
	#[instrument(name = "store::operator::persistent::memory::apply", level = "trace", skip_all)]
	fn apply(&self, batch: &FlushBatch) -> Result<Applied> {
		let mut rows = self.0.rows.lock();
		let mut checkpoints = self.0.checkpoints.lock();
		let mut applied = Applied::default();
		let mut bytes = 0u64;
		for drop in &batch.drops {
			let DropMarker::OperatorState(operator) = drop;
			rows.remove(operator);
		}
		for (operator, key, write) in &batch.writes {
			match write {
				StagedWrite::Set(row) => {
					bytes += row_bytes(key, row);
					rows.entry(*operator).or_default().insert(key.clone(), row.clone());
				}
				StagedWrite::Remove => {
					if let Some(held) = rows.get_mut(operator) {
						held.remove(key);
						if held.is_empty() {
							rows.remove(operator);
						}
					}
				}
			}
			applied.rows += 1;
		}
		for (flow, version) in &batch.checkpoints {
			match version {
				Some(version) => checkpoints.insert(*flow, *version),
				None => checkpoints.remove(flow),
			};
		}
		applied.bytes = ByteSize::from_bytes(bytes);
		Ok(applied)
	}

	#[instrument(name = "store::operator::persistent::memory::drop_operator", level = "trace", skip_all)]
	fn drop_operator(&self, operator: OperatorId) -> Result<()> {
		self.0.rows.lock().remove(&operator);
		Ok(())
	}
}
