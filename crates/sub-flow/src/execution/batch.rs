// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::{
	common::{CommitVersion, TimeDomain},
	interface::{
		catalog::flow::{FlowId, FlowNodeId},
		change::Change,
	},
};
use reifydb_flow::transaction::{ChangeCoordinate, FlowTransaction};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::{Span, field, instrument};

use crate::{engine::FlowEngineInner, execution::retention_instant, operator::max_input_time};

impl FlowEngineInner {
	#[instrument(name = "flow::engine::process", level = "debug", skip(self, txn, change), fields(
		flow_id = ?flow_id,
		origin = ?change.origin,
		version = change.version.0,
		diff_count = change.diffs.len(),
		row_count = change.row_count(),
		nodes_processed = field::Empty
	))]
	pub fn process(&self, txn: &mut FlowTransaction, change: Change, flow_id: FlowId) -> Result<()> {
		self.process_batch(txn, vec![change], flow_id)
	}

	#[instrument(name = "flow::engine::process_batch", level = "debug", skip(self, txn, changes), fields(
		flow_id = ?flow_id,
		batch_change_count = changes.len(),
		batch_row_count = changes.iter().map(Change::row_count).sum::<usize>(),
		version_count = field::Empty,
		nodes_processed = field::Empty
	))]
	pub fn process_batch(&self, txn: &mut FlowTransaction, changes: Vec<Change>, flow_id: FlowId) -> Result<()> {
		let flow = match self.flows.get(&flow_id) {
			Some(f) => f.clone(),
			None => return Ok(()),
		};

		let mut by_version: BTreeMap<CommitVersion, Vec<Change>> = BTreeMap::new();
		for change in changes {
			by_version.entry(change.version).or_default().push(change);
		}
		Span::current().record("version_count", by_version.len());

		let topo = flow.topological_order()?;
		let mut nodes_processed = 0u32;

		for (version, version_changes) in by_version {
			nodes_processed +=
				self.process_version(txn, &flow, flow_id, version, version_changes, &topo)?;
		}

		Span::current().record("nodes_processed", nodes_processed);
		Ok(())
	}

	#[inline]
	fn process_version(
		&self,
		txn: &mut FlowTransaction,
		flow: &FlowDag,
		flow_id: FlowId,
		version: CommitVersion,
		version_changes: Vec<Change>,
		topo: &[FlowNodeId],
	) -> Result<u32> {
		let mut pending: HashMap<FlowNodeId, Vec<Change>> = HashMap::new();
		for change in version_changes {
			self.seed_entry_nodes(flow, flow_id, change, &mut pending);
		}

		let sources: Vec<FlowNodeId> = topo
			.iter()
			.copied()
			.filter(|id| flow.get_node(id).is_some_and(|node| node.ty.is_source()))
			.collect();
		let arrivals: Vec<(FlowNodeId, DateTime)> = pending
			.iter()
			.filter_map(|(node_id, changes)| {
				changes.iter().filter_map(max_input_time).max().map(|at| (*node_id, at))
			})
			.collect();
		freeze_arrival_frontier(txn, flow.time_domain(), &sources, &arrivals)?;

		let mut nodes_processed = self.run_topology(txn, flow, pending, topo)?;
		nodes_processed += self.dispatch_due_timers(txn, flow, version, topo)?;
		Ok(nodes_processed)
	}

	pub(super) fn run_topology(
		&self,
		txn: &mut FlowTransaction,
		flow: &FlowDag,
		mut pending: HashMap<FlowNodeId, Vec<Change>>,
		topo: &[FlowNodeId],
	) -> Result<u32> {
		let mut nodes_processed = 0u32;
		for node_id in topo {
			let inbox = match pending.remove(node_id) {
				Some(v) if !v.is_empty() => v,
				_ => continue,
			};

			let node = match flow.get_node(node_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			let at = inbox
				.iter()
				.filter_map(max_input_time)
				.max()
				.or_else(|| inbox.iter().map(|change| change.changed_at).max())
				.expect("a non-empty inbox carries a time");
			let version = inbox
				.iter()
				.map(|change| change.version)
				.max()
				.expect("a non-empty inbox has a version");
			txn.set_change_coordinate(ChangeCoordinate {
				at: retention_instant(txn, flow, at),
				version,
			});

			let combined_output = self.dispatch_node(txn, &node, inbox)?;
			nodes_processed += 1;
			if combined_output.diffs.is_empty() {
				continue;
			}

			let child_count = node.outputs.len();
			for (child_idx, child_id) in node.outputs.iter().enumerate() {
				if child_idx + 1 == child_count {
					pending.entry(*child_id).or_default().push(combined_output);
					break;
				}
				pending.entry(*child_id).or_default().push(combined_output.clone());
			}
		}
		Ok(nodes_processed)
	}
}

fn freeze_arrival_frontier(
	txn: &mut FlowTransaction,
	domain: TimeDomain,
	sources: &[FlowNodeId],
	arrivals: &[(FlowNodeId, DateTime)],
) -> Result<()> {
	let watermarks = txn.source_watermarks();
	if !sources.is_empty() {
		let frontier = watermarks.flow_watermark(domain, sources, txn)?;
		txn.set_flow_watermark(frontier);
	}
	for (node, at) in arrivals {
		watermarks.advance(*node, txn, *at)?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const SOURCE: FlowNodeId = FlowNodeId(1);

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	#[test]
	fn a_versions_own_rows_do_not_move_the_frontier_the_operators_gate_against() {
		// The admit frontier is snapshotted BEFORE the version's own rows advance the source
		// watermarks, so no row is judged late against a sibling committed alongside it. Reversed,
		// one transaction carrying an hour of history into a 1s window keeps only its last bucket.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);

		freeze_arrival_frontier(&mut txn, TimeDomain::Event, &[SOURCE], &[(SOURCE, at(5_000))]).unwrap();
		freeze_arrival_frontier(&mut txn, TimeDomain::Event, &[SOURCE], &[(SOURCE, at(20_000))]).unwrap();

		assert_eq!(
			txn.flow_watermark(),
			Some(at(5_000)),
			"the frontier must be the one that existed before this version's rows, not after them"
		);

		let watermarks = txn.source_watermarks();
		assert_eq!(
			watermarks.source_watermark(SOURCE, &mut txn).unwrap(),
			at(20_000),
			"the version's rows must still have advanced the source, or the frontier is only stale \
			 because nothing was folded in at all"
		);
	}
}
