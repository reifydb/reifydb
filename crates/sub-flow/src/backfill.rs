// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{CommitVersion, Result, interface::FlowId};
use reifydb_transaction::cdc::CdcQueryTransaction;
use tracing::{debug, info};

use crate::{coordinator::FlowCoordinator, transaction::Pending, worker::Batch};

impl FlowCoordinator {
	pub(crate) fn backfill(&self, flow_id: FlowId, up_to_version: CommitVersion) -> Result<()> {
		if up_to_version.0 == 0 {
			info!(flow_id = flow_id.0, "no backfill needed (version 0)");
			return Ok(());
		}

		// Check existing checkpoint (resume on restart)
		let start_version = {
			let mut query = self.engine.begin_query()?;
			CdcCheckpoint::fetch(&mut query, &flow_id).unwrap_or(CommitVersion(0))
		};

		if start_version >= up_to_version {
			info!(flow_id = flow_id.0, checkpoint = start_version.0, "backfill already complete");
			return Ok(());
		}

		info!(
			flow_id = flow_id.0,
			start_version = start_version.0,
			up_to_version = up_to_version.0,
			"backfilling flow"
		);

		let mut txn = self.engine.begin_command()?;
		let cdc_txn = txn.begin_cdc_query()?;
		let batch = cdc_txn.range(Bound::Excluded(start_version), Bound::Included(up_to_version))?;

		let state_version = self.get_parent_snapshot_version(&txn)?;
		let mut batches = Vec::new();
		for cdc in batch.items {
			let version = cdc.version;
			let changes = self.decode_cdc(&cdc, version)?;
			batches.push(Batch {
				version,
				changes,
			});
		}

		let pending_writes = self.pool.process(batches, state_version)?;
		for (key, pending) in pending_writes.iter_sorted() {
			match pending {
				Pending::Set(value) => {
					txn.set(key, value.clone())?;
				}
				Pending::Remove => {
					txn.remove(key)?;
				}
			}
		}

		// Persist per-flow checkpoint after successful backfill
		CdcCheckpoint::persist(&mut txn, &flow_id, up_to_version)?;

		txn.commit()?;
		debug!(flow_id = flow_id.0, checkpoint = up_to_version.0, "backfill complete");
		Ok(())
	}
}
