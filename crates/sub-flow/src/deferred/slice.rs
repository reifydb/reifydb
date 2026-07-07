// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, ops::Bound};

use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::storage::CdcStore;
use reifydb_codec::encoded::shape::RowShape;
use reifydb_core::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, shape::ShapeId},
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_value::{Result, value::datetime::DateTime};
use smallvec::smallvec;

use crate::{
	deferred::committer::FlowSlice,
	engine::FlowEngineInner,
	transaction::{DeferredParams, FlowTransaction},
};

pub struct SliceConfig {
	pub chunk_size: u64,

	pub checkpoint_lag: u64,
}

pub struct SliceCursor<'a> {
	pub flow_id: FlowId,
	pub source_shapes: &'a BTreeSet<ShapeId>,
	pub cursor: CommitVersion,
	pub durable_cursor: CommitVersion,
}

pub enum SliceStep {
	Idle,

	Commit {
		slice: FlowSlice,
		advance_to: CommitVersion,
		more: bool,
	},

	Skip {
		advance_to: CommitVersion,
		more: bool,
	},
}

pub struct SliceComputer {
	engine: StandardEngine,
}

impl SliceComputer {
	pub fn new(engine: StandardEngine) -> Self {
		Self {
			engine,
		}
	}

	pub fn step(
		&self,
		flow_engine: &mut FlowEngineInner,
		cdc_store: &CdcStore,
		cursor: SliceCursor,
		config: &SliceConfig,
	) -> Result<SliceStep> {
		let safe = self.engine.cdc_producer_watermark();
		if safe > self.engine.done_until() || safe <= cursor.cursor {
			return Ok(SliceStep::Idle);
		}

		let batch = cdc_store.read_range(
			Bound::Excluded(cursor.cursor),
			Bound::Included(safe),
			config.chunk_size,
		)?;
		let items = batch.items;

		let Some(chunk_end) = items.last().map(|c| c.version) else {
			return Ok(self.skip_or_checkpoint(cursor.flow_id, safe, cursor.durable_cursor, false, config));
		};
		let more = chunk_end < safe;

		let changes = collect_flow_changes(&items, cursor.source_shapes);
		if changes.is_empty() {
			return Ok(self.skip_or_checkpoint(
				cursor.flow_id,
				chunk_end,
				cursor.durable_cursor,
				more,
				config,
			));
		}

		let (combined, pending_shapes, view_changes) =
			self.compute(flow_engine, cursor.flow_id, chunk_end, changes)?;

		Ok(SliceStep::Commit {
			slice: FlowSlice {
				combined,
				pending_shapes,
				checkpoints: vec![(cursor.flow_id, chunk_end)],
				positions: Vec::new(),
				checkpoint_deletes: Vec::new(),
				view_changes,
				control_cursor: None,
			},
			advance_to: chunk_end,
			more,
		})
	}

	fn skip_or_checkpoint(
		&self,
		flow_id: FlowId,
		advance_to: CommitVersion,
		durable_cursor: CommitVersion,
		more: bool,
		config: &SliceConfig,
	) -> SliceStep {
		if advance_to.0.saturating_sub(durable_cursor.0) > config.checkpoint_lag {
			let mut slice = FlowSlice::empty();
			slice.checkpoints.push((flow_id, advance_to));
			SliceStep::Commit {
				slice,
				advance_to,
				more,
			}
		} else {
			SliceStep::Skip {
				advance_to,
				more,
			}
		}
	}

	fn compute(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		state_version: CommitVersion,
		changes: Vec<Change>,
	) -> Result<(Pending, Vec<RowShape>, Vec<Change>)> {
		let catalog: Catalog = self.engine.catalog();
		let interceptors = self.engine.create_interceptors();

		let (_current, state_lease) = self.engine.acquire_current_snapshot_lease()?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let dictionary_query = self.engine.multi().begin_query()?;

		let mut query = base_query;
		query.read_as_of_version_inclusive(state_version);

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query,
			state_query,
			dictionary_query: Some(dictionary_query),
			single: self.engine.single().clone(),
			catalog,
			interceptors,
			clock: self.engine.clock().clone(),
			allocators: flow_engine.allocators.clone(),
		});

		flow_engine.process_batch(&mut txn, changes, flow_id)?;
		txn.flush_operator_states()?;

		let mut view_changes = Vec::new();
		let changed_at = DateTime::from_nanos(self.engine.clock().now_nanos());
		for (id, diff) in txn.take_accumulator_entries() {
			view_changes.push(Change {
				origin: ChangeOrigin::Shape(id),
				version: state_version,
				diffs: smallvec![diff],
				changed_at,
			});
		}

		let pending_shapes = txn.take_pending_shapes();
		let pending = txn.take_pending();
		Ok((pending, pending_shapes, view_changes))
	}

	pub fn tick(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		timestamp: DateTime,
	) -> Result<(Pending, Vec<RowShape>)> {
		let (state_version, lease) = self.engine.acquire_current_snapshot_lease()?;
		let query = self.engine.multi().begin_query_at_version(&lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&lease)?;
		let dictionary_query = self.engine.multi().begin_query()?;

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query,
			state_query,
			dictionary_query: Some(dictionary_query),
			single: self.engine.single().clone(),
			catalog: self.engine.catalog(),
			interceptors: self.engine.create_interceptors(),
			clock: self.engine.clock().clone(),
			allocators: flow_engine.allocators.clone(),
		});

		flow_engine.process_tick(&mut txn, flow_id, timestamp)?;
		txn.flush_operator_states()?;
		Ok((txn.take_pending(), txn.take_pending_shapes()))
	}
}

fn collect_flow_changes(cdcs: &[Cdc], source_shapes: &BTreeSet<ShapeId>) -> Vec<Change> {
	let mut out = Vec::new();
	for cdc in cdcs {
		for change in &cdc.changes {
			let relevant = match change.origin {
				ChangeOrigin::Shape(shape) => source_shapes.contains(&shape),
				ChangeOrigin::Flow(_) => true,
			};
			if relevant {
				out.push(change.clone());
			}
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			catalog::{
				flow::FlowNodeId,
				id::{TableId, ViewId},
			},
			change::Diff,
		},
		value::column::columns::Columns,
	};

	use super::*;

	fn change(origin: ChangeOrigin, version: u64) -> Change {
		Change {
			origin,
			version: CommitVersion(version),
			diffs: smallvec![Diff::Insert {
				post: Columns::empty(),
				origin: None,
			}],
			changed_at: DateTime::default(),
		}
	}

	fn cdc(version: u64, changes: Vec<Change>) -> Cdc {
		Cdc {
			version: CommitVersion(version),
			timestamp: DateTime::default(),
			changes,
			system_changes: Vec::new(),
		}
	}

	#[test]
	fn shape_changes_match_source_shapes() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(
			5,
			vec![
				change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))), 5),
				change(ChangeOrigin::Shape(ShapeId::Table(TableId(2))), 5),
				change(ChangeOrigin::Shape(ShapeId::View(ViewId(9))), 5),
			],
		)];

		let out = collect_flow_changes(&cdcs, &sources);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Shape(ShapeId::Table(TableId(1)))));
	}

	#[test]
	fn flow_origin_changes_always_included() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(5, vec![change(ChangeOrigin::Flow(FlowNodeId(42)), 5)])];

		let out = collect_flow_changes(&cdcs, &sources);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Flow(FlowNodeId(42))));
	}

	#[test]
	fn unrelated_shape_changes_excluded() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(2))), 5)]),
			cdc(6, vec![change(ChangeOrigin::Shape(ShapeId::View(ViewId(3))), 6)]),
		];

		let out = collect_flow_changes(&cdcs, &sources);

		assert!(out.is_empty());
	}

	#[test]
	fn changes_gathered_across_multiple_cdc_entries_in_order() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))), 5)]),
			cdc(7, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))), 7)]),
		];

		let out = collect_flow_changes(&cdcs, &sources);

		assert_eq!(out.len(), 2);
		assert_eq!(out[0].version, CommitVersion(5));
		assert_eq!(out[1].version, CommitVersion(7));
	}
}

#[cfg(test)]
mod integration {
	use std::{collections::HashMap, thread::sleep, time::Duration as StdDuration};

	use reifydb_core::interface::WithEventBus;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::{
		builder::CustomOperators,
		catalog::FlowCatalog,
		deferred::{committer::Committer, routing, tracker::FlowPositionTracker},
		transaction::allocators::FlowAllocators,
	};

	fn view_row_count(te: &TestEngine, rql: &str) -> usize {
		te.query(rql).first().map(|f| f.row_count()).unwrap_or(0)
	}

	fn build_flow_engine(engine: &StandardEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			CustomOperators::new(HashMap::new()),
			FlowAllocators::with_dictionary(engine.dictionary_allocators()),
		)
	}

	#[test]
	fn deferred_view_materializes_through_slice_step() {
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}, {id: 3, val: 30}]");

		let engine = te.inner().clone();
		let cdc_store = engine.cdc_store();
		let flow_catalog = FlowCatalog::new(engine.catalog());

		// Discover the single deferred flow and register it into a fresh per-flow engine.
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flows = engine.catalog().list_flows_all(&mut Transaction::Query(&mut query)).expect("list flows");
		let flow_id = flows.first().expect("one flow").id;
		drop(query);

		let mut flow_engine = build_flow_engine(&engine);
		{
			let mut txn = engine.begin_command(IdentityId::system()).expect("command");
			let (flow, _) = flow_catalog
				.get_or_load_flow(&mut Transaction::Command(&mut txn), flow_id)
				.expect("load flow");
			flow_engine.register(&mut txn, flow).expect("register");
			txn.rollback().expect("rollback registration probe");
		}

		let source_shapes = {
			let graph = flow_engine.analyzer.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_route = |vid| {
				flow_catalog.find_view(vid).map(|v| routing::ViewRoute {
					kind: v.kind(),
					underlying: v.underlying_id(),
				})
			};
			routing::flow_source_shapes(graph, flow_id, &registered, &view_route)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = Committer::new(engine.clone(), flow_catalog, FlowPositionTracker::new());
		let config = SliceConfig {
			chunk_size: 1000,
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut durable = CommitVersion(0);
		let mut committed_any = false;

		// CDC production is async; spin the drain, letting the producer catch up, until the
		// view materializes or we exhaust the budget.
		for _ in 0..400 {
			match computer
				.step(
					&mut flow_engine,
					&cdc_store,
					SliceCursor {
						flow_id,
						source_shapes: &source_shapes,
						cursor,
						durable_cursor: durable,
					},
					&config,
				)
				.expect("step")
			{
				SliceStep::Commit {
					slice,
					advance_to,
					..
				} => {
					committer.commit_slice(slice).expect("commit slice");
					cursor = advance_to;
					durable = advance_to;
					committed_any = true;
				}
				SliceStep::Skip {
					advance_to,
					..
				} => {
					cursor = advance_to;
				}
				SliceStep::Idle => {
					if view_row_count(&te, "FROM app::v") == 3 {
						break;
					}
					sleep(StdDuration::from_millis(5));
				}
			}
		}

		assert!(committed_any, "expected at least one slice to commit view rows");
		let frames = te.query("FROM app::v");
		assert_eq!(
			frames.first().map(|f| f.row_count()).unwrap_or(0),
			3,
			"deferred view should materialize all three source rows"
		);
	}
}
