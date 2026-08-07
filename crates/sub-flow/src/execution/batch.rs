// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::{FlowId, OperatorId},
			object::ObjectId,
		},
		change::{Change, ChangeOrigin},
	},
};
use reifydb_flow::transaction::{ChangeCoordinate, FlowTransaction};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::{
	Result, reifydb_assertions,
	value::{Value, datetime::DateTime},
};
use tracing::{Span, field, instrument};

use crate::{engine::FlowEngineInner, execution::COMPLETENESS_OBJECT, operator::max_input_time};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceArrival {
	pub source: OperatorId,
	pub at: DateTime,
}

pub(crate) type SourceArrivals = Vec<SourceArrival>;

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
		topo: &[OperatorId],
	) -> Result<u32> {
		let mut pending: HashMap<OperatorId, Vec<Change>> = HashMap::new();
		let mut asserted: BTreeMap<u64, DateTime> = BTreeMap::new();
		for change in version_changes {
			if change.origin == ChangeOrigin::Object(COMPLETENESS_OBJECT) {
				collect_completeness(&change, &mut asserted);
				continue;
			}
			self.seed_entry_nodes(flow, flow_id, change, &mut pending);
		}

		let sources: Vec<OperatorId> = topo
			.iter()
			.copied()
			.filter(|id| flow.get_operator(id).is_some_and(|operator| operator.ty.declares_time()))
			.collect();
		let mut arrivals: SourceArrivals = pending
			.iter()
			.filter_map(|(operator_id, changes)| {
				changes.iter().filter_map(max_input_time).max().map(|at| SourceArrival {
					source: *operator_id,
					at,
				})
			})
			.collect();
		arrivals.extend(completeness_arrivals(&self.sources, flow_id, &asserted));
		freeze_arrival_frontier(txn, &sources, &arrivals)?;

		let mut nodes_processed = self.run_topology(txn, flow, pending, topo)?;
		nodes_processed += self.dispatch_due_timers(txn, flow, version, topo)?;
		Ok(nodes_processed)
	}

	pub(super) fn run_topology(
		&self,
		txn: &mut FlowTransaction,
		flow: &FlowDag,
		mut pending: HashMap<OperatorId, Vec<Change>>,
		topo: &[OperatorId],
	) -> Result<u32> {
		let mut nodes_processed = 0u32;
		for operator_id in topo {
			let inbox = match pending.remove(operator_id) {
				Some(v) if !v.is_empty() => v,
				_ => continue,
			};

			let operator = match flow.get_operator(operator_id) {
				Some(n) => n.clone(),
				None => continue,
			};

			let at = inbox.iter().filter_map(max_input_time).max();
			let version = inbox
				.iter()
				.map(|change| change.version)
				.max()
				.expect("a non-empty inbox has a version");
			txn.set_change_coordinate(ChangeCoordinate {
				at,
				version,
			});

			let combined_output = self.dispatch_node(txn, &operator, inbox)?;
			nodes_processed += 1;
			if combined_output.diffs.is_empty() {
				continue;
			}

			let child_count = operator.outputs.len();
			for (child_idx, child_id) in operator.outputs.iter().enumerate() {
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

fn completeness_arrivals(
	sources: &BTreeMap<ObjectId, Vec<(FlowId, OperatorId)>>,
	flow_id: FlowId,
	asserted: &BTreeMap<u64, DateTime>,
) -> SourceArrivals {
	let mut out = Vec::new();
	for (object, at) in asserted {
		let resolved: Vec<&Vec<(FlowId, OperatorId)>> = sources
			.iter()
			.filter_map(|(source, registrations)| (*source == *object).then_some(registrations))
			.collect();

		reifydb_assertions! {
			assert!(
				resolved.len() <= 1,
				"every source id is drawn from one sequence, so {object} must name at most \
				 one object; advancing the wrong source is irreversible"
			);
		}

		for registrations in resolved {
			for (registered_flow_id, operator_id) in registrations {
				if *registered_flow_id == flow_id {
					out.push(SourceArrival {
						source: *operator_id,
						at: *at,
					});
				}
			}
		}
	}
	out
}

fn collect_completeness(change: &Change, asserted: &mut BTreeMap<u64, DateTime>) {
	for diff in change.diffs.iter() {
		let Some(columns) = diff.post() else {
			continue;
		};
		let (Some(objects), Some(instants)) = (columns.column("object_id"), columns.column("complete_through"))
		else {
			continue;
		};
		for row in 0..columns.row_count() {
			let (Value::Uint8(object), Value::DateTime(at)) =
				(objects.data().get_value(row), instants.data().get_value(row))
			else {
				continue;
			};

			reifydb_assertions! {
				assert!(
					!asserted.contains_key(&object),
					"the table holds one row per object, so {object} must not assert twice \
					 in one version"
				);
			}

			let slot = asserted.entry(object).or_insert(at);
			*slot = (*slot).max(at);
		}
	}
}

fn freeze_arrival_frontier(
	txn: &mut FlowTransaction,
	sources: &[OperatorId],
	arrivals: &[SourceArrival],
) -> Result<()> {
	let watermarks = txn.source_watermarks();
	if !sources.is_empty() {
		let frontier = watermarks.flow_watermark(sources, txn)?;
		txn.set_flow_watermark(frontier);
	}
	for arrival in arrivals {
		watermarks.advance(arrival.source, txn, arrival.at)?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		common::TimeDomain,
		interface::{
			WithEventBus,
			catalog::id::{SeriesId, TableId},
			change::Diff,
		},
		state::budget::OperatorStateBudgetHandle,
		value::column::columns::Columns,
	};
	use reifydb_flow::transaction::substrate::FlowSubstrate;
	use reifydb_rql::flow::operator::{FlowNode, OperatorDef};
	use reifydb_runtime::context::{
		RuntimeContext,
		clock::{Clock, MockClock},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{factory::time::at_millis, value::identity::IdentityId};
	use smallvec::smallvec;

	use super::*;
	use crate::{builder::CustomOperators, operator::metrics::OperatorSampleRegistry};

	const SOURCE: OperatorId = OperatorId(1);

	fn completeness_change(rows: &[(u64, DateTime)]) -> Change {
		let post = Columns::from_rows(
			&["object_id", "complete_through"],
			&rows.iter().map(|(o, at)| vec![Value::Uint8(*o), Value::DateTime(*at)]).collect::<Vec<_>>(),
		);
		Change {
			origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
			version: CommitVersion(1),
			diffs: smallvec![Diff::insert(post)],
			changed_at: DateTime::default(),
		}
	}

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

	#[test]
	fn an_assertion_is_read_from_the_post_image() {
		let mut asserted = BTreeMap::new();

		collect_completeness(&completeness_change(&[(7, at_millis(9_000))]), &mut asserted);

		assert_eq!(asserted, BTreeMap::from([(7u64, at_millis(9_000))]));
	}

	#[test]
	fn a_deleted_completeness_row_asserts_nothing() {
		// A watermark never retracts, so a delete must not be read as an assertion of its old value.
		let pre = Columns::from_rows(
			&["object_id", "complete_through"],
			&[vec![Value::Uint8(7), Value::DateTime(at_millis(9_000))]],
		);
		let change = Change {
			origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
			version: CommitVersion(1),
			diffs: smallvec![Diff::remove(pre)],
			changed_at: DateTime::default(),
		};
		let mut asserted = BTreeMap::new();

		collect_completeness(&change, &mut asserted);

		assert!(asserted.is_empty());
	}

	#[test]
	fn an_assertion_resolves_across_object_kinds_and_only_for_the_asserting_flow() {
		// The column carries no kind, so a resolver keyed on TableId alone never finds a Series.
		let sources = BTreeMap::from([(
			ObjectId::Series(SeriesId(9)),
			vec![(FlowId(1), OperatorId(3)), (FlowId(2), OperatorId(4))],
		)]);
		let asserted = BTreeMap::from([(9u64, at_millis(9_000))]);

		let arrivals = completeness_arrivals(&sources, FlowId(1), &asserted);

		assert_eq!(
			arrivals,
			vec![SourceArrival {
				source: OperatorId(3),
				at: at_millis(9_000)
			}]
		);
	}

	#[test]
	fn an_assertion_for_an_object_this_flow_does_not_read_yields_no_arrival() {
		let sources = BTreeMap::from([(ObjectId::Table(TableId(5)), vec![(FlowId(2), OperatorId(4))])]);
		let asserted = BTreeMap::from([(5u64, at_millis(9_000))]);

		let arrivals = completeness_arrivals(&sources, FlowId(1), &asserted);

		assert!(arrivals.is_empty());
	}

	#[test]
	fn process_version_folds_an_assertion_into_the_frontier() {
		// Without the fold reaching freeze_arrival_frontier the assertion is decoded and discarded.
		let engine = TestEngine::new();
		let mut inner = FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowSubstrate::default(),
			OperatorSampleRegistry::new(),
			OperatorStateBudgetHandle::default(),
		);
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(
			SOURCE,
			OperatorDef::SourceSeries {
				series: SeriesId(9),
				time_domain: TimeDomain::None,
			},
		));
		let flow = builder.build();
		inner.register_flow_dag(flow.clone());
		inner.sources.insert(ObjectId::Series(SeriesId(9)), vec![(FlowId(1), SOURCE)]);

		let mut txn = deferred(&engine);
		let topo = flow.topological_order().unwrap();
		inner.process_version(
			&mut txn,
			&flow,
			FlowId(1),
			CommitVersion(1),
			vec![completeness_change(&[(9, at_millis(30_000))])],
			&topo,
		)
		.unwrap();

		let watermarks = txn.source_watermarks();
		assert_eq!(
			watermarks.source_watermark(SOURCE, &mut txn).unwrap(),
			at_millis(30_000),
			"the assertion must reach the frontier through process_version, not only the helper"
		);
	}

	#[test]
	fn an_assertion_advances_a_source_that_produced_no_rows_in_the_commit() {
		// A silent source pins the min-merged frontier at the epoch forever without an assertion.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let sources = BTreeMap::from([(ObjectId::Series(SeriesId(9)), vec![(FlowId(1), SOURCE)])]);
		let asserted = BTreeMap::from([(9u64, at_millis(30_000))]);

		let arrivals = completeness_arrivals(&sources, FlowId(1), &asserted);
		freeze_arrival_frontier(&mut txn, &[SOURCE], &arrivals).unwrap();

		let watermarks = txn.source_watermarks();
		assert_eq!(
			watermarks.source_watermark(SOURCE, &mut txn).unwrap(),
			at_millis(30_000),
			"an assertion with no rows in the commit must still advance the source"
		);
	}

	#[test]
	fn a_versions_own_rows_do_not_move_the_frontier_the_operators_gate_against() {
		// The admit frontier is snapshotted BEFORE the version's own rows advance the source
		// watermarks, so no row is judged late against a sibling committed alongside it. Reversed,
		// one transaction carrying an hour of history into a 1s window keeps only its last bucket.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);

		freeze_arrival_frontier(
			&mut txn,
			&[SOURCE],
			&[SourceArrival {
				source: SOURCE,
				at: at_millis(5_000),
			}],
		)
		.unwrap();
		freeze_arrival_frontier(
			&mut txn,
			&[SOURCE],
			&[SourceArrival {
				source: SOURCE,
				at: at_millis(20_000),
			}],
		)
		.unwrap();

		assert_eq!(
			txn.flow_watermark(),
			Some(at_millis(5_000)),
			"the frontier must be the one that existed before this version's rows, not after them"
		);

		let watermarks = txn.source_watermarks();
		assert_eq!(
			watermarks.source_watermark(SOURCE, &mut txn).unwrap(),
			at_millis(20_000),
			"the version's rows must still have advanced the source, or the frontier is only stale \
			 because nothing was folded in at all"
		);
	}
}
