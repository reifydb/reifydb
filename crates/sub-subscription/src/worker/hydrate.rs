// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, result::Result as StdResult};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, id::SubscriptionId, shape::ShapeId},
		change::{Change, Diff},
	},
	metrics::execution::{ExecutionMetrics, StatementMetrics},
	value::column::columns::Columns,
};
use reifydb_engine::subscription::{HydrateError, HydrateOutcome};
use reifydb_rql::fingerprint::request::fingerprint_request;
use reifydb_runtime::context::clock::Instant;
use reifydb_sub_flow::transaction::FlowTransaction;
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId},
};

use super::{SubscriptionWorkerActor, SubscriptionWorkerState};
use crate::subsystem::hydration::{collect_source_descriptors, run_source_queries};

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

		let now = DateTime::from_nanos(self.engine.clock().now_nanos());
		self.apply_source_frames(state, flow_id, version, source_frames, now)?;

		self.store.begin_hydration(sub_id);
		self.delivery.commit_batch();
		drop(outer);

		Ok(self.build_outcome(sub_id, version, hydrate_start, statements))
	}

	fn apply_source_frames(
		&self,
		state: &mut SubscriptionWorkerState,
		flow_id: FlowId,
		version: CommitVersion,
		source_frames: Vec<(ShapeId, Vec<Columns>)>,
		now: DateTime,
	) -> Result<()> {
		let SubscriptionWorkerState {
			flow_engine,
			flows,
		} = state;
		let flow_state = flows.get_mut(&flow_id).expect("hydrated flow registered");

		let keyed = mem::take(&mut flow_state.keyed_state);
		let operators = mem::take(&mut flow_state.operator_states);

		let mut txn = FlowTransaction::ephemeral(
			version,
			self.engine.multi().begin_query()?,
			self.engine.single_owned(),
			self.catalog.clone(),
			keyed,
			flow_engine.clock().clone(),
		);
		txn.install_operator_states(operators);

		for (shape, shape_columns) in source_frames {
			for columns in shape_columns {
				for row_idx in 0..columns.row_count() {
					let row = columns.extract_row(row_idx);
					let diff = Diff::insert(row);
					let change = Change::from_shape(shape, version, vec![diff], now);
					flow_engine.process(&mut txn, change, flow_id)?;
				}
			}
		}

		txn.merge_state();
		flow_state.keyed_state = txn.take_state();
		flow_state.operator_states = txn.drain_operator_states();
		Ok(())
	}

	fn build_outcome(
		&self,
		sub_id: SubscriptionId,
		version: CommitVersion,
		hydrate_start: Instant,
		statements: Vec<StatementMetrics>,
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

		let batches = self.store.drain(&sub_id, usize::MAX);
		self.store.end_hydration(&sub_id);
		HydrateOutcome {
			version,
			batches,
			metrics,
		}
	}
}
