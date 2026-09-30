// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, num::NonZeroU64, result::Result as StdResult};

use arrow_array::RecordBatch;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::FlowId,
			id::SubscriptionId,
		},
		change::{Change, StagedBatch},
	},
	internal_err,
	value::batch::{concat, take_rows},
};
use reifydb_engine::{
	backfill,
	subscription::{HydrateError, HydrateOutcome},
};
use reifydb_flow_async::engine::FlowEngineInner;
use reifydb_runtime::context::clock::Instant;
use reifydb_transaction::{
	multi::{lease::VersionLeaseGuard, transaction::read::MultiReadTransaction},
	transaction::Transaction,
};
use reifydb_value::{
	Result,
	value::{
		diff_type::DiffType, duration::Duration, identity::IdentityId, row_number::RowNumber,
		system_columns::require_row_numbers,
	},
};

use super::{SubscriptionFlowState, SubscriptionWorkerActor, SubscriptionWorkerState};
use crate::{
	delivery::hydration::{backfill_sources, hydration_bound},
	store::HydrationGuard,
	transaction::EphemeralTransaction,
};

impl SubscriptionWorkerActor {
	pub(super) fn run_hydrate(
		&self,
		state: &mut SubscriptionWorkerState,
		sub_id: SubscriptionId,
		flow_id: FlowId,
		identity: IdentityId,
		lease: VersionLeaseGuard,
		max_rows: u64,
	) -> StdResult<HydrateOutcome, HydrateError> {
		let hydrate_start = self.engine.clock().instant();
		let Some(flow_state) = state.flows.get(&flow_id).filter(|flow_state| flow_state.identity == identity)
		else {
			return Err(HydrateError::SubscriptionNotFound);
		};
		if flow_state.held.is_none() {
			return Err(HydrateError::Internal(format!(
				"subscription {} is already live, so a second snapshot would announce its rows twice",
				sub_id.0
			)));
		}

		let flow = state.flow_engine.flow_by_id(flow_id).ok_or(HydrateError::SubscriptionNotFound)?;
		let sources = backfill_sources(&flow)?;
		let batch_size = row_batch_size(&self.catalog)?;
		let version = lease.version();
		let base_query = self.engine.multi().begin_query_at_version(&lease)?;
		let mut scan = self.engine.begin_query_at_version(&lease, identity)?;

		let _hydration = HydrationGuard::new(&self.store, sub_id);

		let mut snapshot = Snapshot::default();
		let mut over_cap = false;
		let backfilled = {
			let SubscriptionWorkerState {
				flow_engine,
				flows,
				..
			} = &mut *state;
			let flow_state = flows.get_mut(&flow_id).expect("hydrated flow registered");
			backfill::run(
				&self.engine.services(),
				Transaction::Query(&mut scan),
				&sources,
				batch_size,
				|_, change| {
					self.apply_chunk(flow_engine, flow_state, &base_query, change, flow_id)?;
					snapshot.fold(self.delivery.take_staged(sub_id))?;
					if snapshot.len() > max_rows {
						over_cap = true;
						return internal_err!(
							"the backfill of subscription {} passed its cap of {} rows",
							sub_id.0,
							max_rows
						);
					}
					Ok(())
				},
			)
		};
		let batches = match backfilled {
			Ok(_) => snapshot.into_batches().map_err(HydrateError::from),
			Err(_) if over_cap => Err(HydrateError::RowCapExceeded {
				cap: max_rows,
				bound: hydration_bound(&flow),
			}),
			Err(e) => Err(HydrateError::from(e)),
		};
		let batches = batches.inspect_err(|_| self.discard_flow(state, sub_id, flow_id))?;

		let flow_state = state.flows.get_mut(&flow_id).expect("hydrated flow registered");
		flow_state.gate = version;
		let held = flow_state.held.take().unwrap_or_default();
		self.replay_held(state, flow_id, &base_query, &held);
		self.delivery.commit_for(sub_id);

		Ok(self.build_outcome(version, hydrate_start, batches))
	}

	fn apply_chunk(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_state: &mut SubscriptionFlowState,
		base_query: &MultiReadTransaction,
		change: Change,
		flow_id: FlowId,
	) -> Result<()> {
		let mut txn = EphemeralTransaction::new(
			change.version.commit,
			base_query.clone(),
			self.catalog.clone(),
			mem::take(&mut flow_state.keyed_state),
			flow_engine.clock().clone(),
			flow_engine.substrate().clone(),
		);
		flow_engine.process(&mut txn, change, flow_id)?;
		txn.merge_state();
		flow_state.keyed_state = txn.take_state();
		Ok(())
	}

	fn discard_flow(&self, state: &mut SubscriptionWorkerState, sub_id: SubscriptionId, flow_id: FlowId) {
		state.flows.remove(&flow_id);
		state.flow_engine.remove_flow(flow_id);
		self.delivery.take_staged(sub_id);
	}

	fn build_outcome(
		&self,
		version: CommitVersion,
		hydrate_start: Instant,
		batches: Vec<StagedBatch>,
	) -> HydrateOutcome {
		let elapsed = hydrate_start.elapsed();
		let elapsed_nanos = elapsed.as_nanos() as i64;
		let total = Duration::from_nanoseconds(elapsed_nanos).unwrap_or_default();

		HydrateOutcome {
			version,
			batches,
			total,
		}
	}
}

fn row_batch_size(catalog: &Catalog) -> Result<NonZeroU64> {
	match NonZeroU64::new(u64::from(catalog.get_config_uint2(ConfigKey::QueryRowBatchSize))) {
		Some(batch_size) => Ok(batch_size),
		None => internal_err!("QUERY_ROW_BATCH_SIZE is 0, which its config validation rejects"),
	}
}

#[derive(Default)]
struct Snapshot {
	batches: Vec<RecordBatch>,
	rows: HashMap<RowNumber, (usize, usize)>,
	stored: usize,
}

impl Snapshot {
	fn len(&self) -> u64 {
		self.rows.len() as u64
	}

	fn fold(&mut self, staged: Vec<StagedBatch>) -> Result<()> {
		for (op, batch) in staged {
			if batch.num_rows() == 0 {
				continue;
			}
			match op {
				DiffType::Insert | DiffType::Update => {
					let at = self.batches.len();
					for (index, row_number) in require_row_numbers(&batch)?.iter().enumerate() {
						self.rows.insert(*row_number, (at, index));
					}
					self.stored += batch.num_rows();
					self.batches.push(batch);
				}
				DiffType::Remove => {
					for row_number in require_row_numbers(&batch)? {
						self.rows.remove(row_number);
					}
				}
			}
		}
		if self.stored > 2 * self.rows.len() {
			self.compact()?;
		}
		Ok(())
	}

	fn compact(&mut self) -> Result<()> {
		let mut live: Vec<(usize, usize)> = self.rows.values().copied().collect();
		live.sort_unstable();
		let mut parts: Vec<RecordBatch> = Vec::new();
		for run in live.chunk_by(|left, right| left.0 == right.0) {
			let indices: Vec<usize> = run.iter().map(|(_, index)| *index).collect();
			parts.push(take_rows(&self.batches[run[0].0], &indices)?);
		}
		let mut batches: Vec<RecordBatch> = Vec::new();
		for run in parts.chunk_by(same_columns) {
			batches.push(concat(run)?);
		}
		self.rows.clear();
		for (at, batch) in batches.iter().enumerate() {
			for (index, row_number) in require_row_numbers(batch)?.iter().enumerate() {
				self.rows.insert(*row_number, (at, index));
			}
		}
		self.stored = live.len();
		self.batches = batches;
		Ok(())
	}

	fn into_batches(mut self) -> Result<Vec<StagedBatch>> {
		self.compact()?;
		let mut newest_first = Vec::with_capacity(self.batches.len());
		for batch in self.batches.iter().rev() {
			let indices: Vec<usize> = (0..batch.num_rows()).rev().collect();
			newest_first.push((DiffType::Insert, take_rows(batch, &indices)?));
		}
		Ok(newest_first)
	}
}

fn same_columns(left: &RecordBatch, right: &RecordBatch) -> bool {
	left.schema_ref().fields().iter().map(|field| field.name()).eq(right
		.schema_ref()
		.fields()
		.iter()
		.map(|field| field.name()))
}
