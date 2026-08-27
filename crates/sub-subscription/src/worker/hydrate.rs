// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, result::Result as StdResult};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, id::SubscriptionId, object::ObjectId},
		change::{Change, Diff, StagedBatch},
	},
	metrics::execution::{ExecutionMetrics, StatementMetrics},
	value::column::columns::Columns,
};
use reifydb_engine::subscription::{HydrateError, HydrateOutcome};
use reifydb_rql::fingerprint::request::fingerprint_request;
use reifydb_runtime::context::clock::Instant;
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};

use super::{SubscriptionWorkerActor, SubscriptionWorkerState};
use crate::{
	delivery::hydration::{collect_source_descriptors, run_source_queries},
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
		if !state.flows.contains_key(&flow_id) {
			return Err(HydrateError::SubscriptionNotFound);
		}

		let version = lease.version();
		if let Some(flow_state) = state.flows.get_mut(&flow_id) {
			flow_state.gate = Some(version);
		}
		let hydrate_start = self.engine.clock().instant();

		let flow = state.flow_engine.flow_by_id(flow_id).ok_or(HydrateError::SubscriptionNotFound)?;
		let mut outer = self.engine.begin_query_at_version(&lease, identity)?;
		let sources = collect_source_descriptors(&flow, &self.catalog, &mut outer)?;
		let (source_frames, statements) = run_source_queries(&self.engine, &mut outer, sources, max_rows)?;

		self.store.begin_hydration(sub_id);

		let now = self.engine.clock().now();
		self.apply_source_frames(state, flow_id, version, source_frames, now)?;

		let batches = self.delivery.take_staged(sub_id);
		self.delivery.commit_batch();
		drop(outer);

		Ok(self.build_outcome(sub_id, version, hydrate_start, statements, batches))
	}

	fn apply_source_frames(
		&self,
		state: &mut SubscriptionWorkerState,
		flow_id: FlowId,
		version: CommitVersion,
		source_frames: Vec<(ObjectId, Vec<Columns>)>,
		now: DateTime,
	) -> Result<()> {
		let SubscriptionWorkerState {
			flow_engine,
			flows,
			..
		} = state;
		let flow_state = flows.get_mut(&flow_id).expect("hydrated flow registered");

		let keyed = mem::take(&mut flow_state.keyed_state);

		let mut txn = EphemeralTransaction::new(
			version,
			self.engine.multi().begin_query()?,
			self.catalog.clone(),
			keyed,
			flow_engine.clock().clone(),
			flow_engine.substrate().clone(),
		);

		let mut changes = Vec::with_capacity(source_frames.len());
		for (shape, shape_columns) in source_frames {
			let diffs: Vec<Diff> =
				shape_columns.into_iter().filter(|c| c.row_count() > 0).map(Diff::insert).collect();
			if diffs.is_empty() {
				continue;
			}
			changes.push(Change::from_object(shape, version, diffs, now));
		}
		if !changes.is_empty() {
			flow_engine.process_batch(&mut txn, changes, flow_id)?;
		}

		txn.merge_state();
		flow_state.keyed_state = txn.take_state();
		Ok(())
	}

	fn build_outcome(
		&self,
		sub_id: SubscriptionId,
		version: CommitVersion,
		hydrate_start: Instant,
		statements: Vec<StatementMetrics>,
		batches: Vec<StagedBatch>,
	) -> HydrateOutcome {
		let elapsed = hydrate_start.elapsed();
		let elapsed_nanos = elapsed.as_nanos() as i64;
		let total = Duration::from_nanoseconds(elapsed_nanos).unwrap_or_default();
		let fps: Vec<_> = statements.iter().map(|m| m.fingerprint).collect();
		let metrics = ExecutionMetrics {
			fingerprint: fingerprint_request(&fps),
			statements,
			total,
			compute: total,
		};

		self.store.end_hydration(&sub_id);
		HydrateOutcome {
			version,
			batches,
			metrics,
		}
	}
}
