// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, ops::Bound, sync::Arc};

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
	deferred::{committer::FlowSlice, overlay::FlowWriteOverlay},
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

struct SliceBatch<'a> {
	items: &'a [&'a Cdc],
	chunk_end: CommitVersion,
	more: bool,
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
		overlay: &mut FlowWriteOverlay,
	) -> Result<SliceStep> {
		let safe = self.engine.cdc_producer_watermark().min(self.engine.done_until());
		if safe <= cursor.cursor {
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
		let items: Vec<&Cdc> = items.iter().collect();

		self.process_items(
			flow_engine,
			&cursor,
			SliceBatch {
				items: &items,
				chunk_end,
				more,
			},
			config,
			overlay,
		)
	}

	fn process_items(
		&self,
		flow_engine: &mut FlowEngineInner,
		cursor: &SliceCursor,
		batch: SliceBatch,
		config: &SliceConfig,
		overlay: &mut FlowWriteOverlay,
	) -> Result<SliceStep> {
		let SliceBatch {
			items,
			chunk_end,
			more,
		} = batch;
		let changes = collect_flow_changes(items, cursor.source_shapes);
		if changes.is_empty() {
			return Ok(self.skip_or_checkpoint(
				cursor.flow_id,
				chunk_end,
				cursor.durable_cursor,
				more,
				config,
			));
		}

		overlay.prune_through(chunk_end);
		let (combined, pending_shapes, view_changes) =
			self.compute(flow_engine, cursor.flow_id, chunk_end, changes, overlay.merged())?;

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

	pub fn step_pushed(
		&self,
		flow_engine: &mut FlowEngineInner,
		segments: &[Arc<Vec<Cdc>>],
		cursor: SliceCursor,
		config: &SliceConfig,
		overlay: &mut FlowWriteOverlay,
	) -> Result<SliceStep> {
		let mut items: Vec<&Cdc> = Vec::new();
		for segment in segments {
			let start = segment.partition_point(|c| c.version <= cursor.cursor);
			items.extend(segment[start..].iter());
		}

		let Some(chunk_end) = items.last().map(|c| c.version) else {
			return Ok(SliceStep::Idle);
		};

		self.process_items(
			flow_engine,
			&cursor,
			SliceBatch {
				items: &items,
				chunk_end,
				more: false,
			},
			config,
			overlay,
		)
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
		base_pending: Arc<Pending>,
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
			base_pending,
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
			base_pending: Arc::new(Pending::new()),
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

fn collect_flow_changes(cdcs: &[&Cdc], source_shapes: &BTreeSet<ShapeId>) -> Vec<Change> {
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

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Shape(ShapeId::Table(TableId(1)))));
	}

	#[test]
	fn flow_origin_changes_always_included() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(5, vec![change(ChangeOrigin::Flow(FlowNodeId(42)), 5)])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources);

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

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources);

		assert!(out.is_empty());
	}

	#[test]
	fn changes_gathered_across_multiple_cdc_entries_in_order() {
		let sources: BTreeSet<ShapeId> = [ShapeId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))), 5)]),
			cdc(7, vec![change(ChangeOrigin::Shape(ShapeId::Table(TableId(1))), 7)]),
		];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources);

		assert_eq!(out.len(), 2);
		assert_eq!(out[0].version, CommitVersion(5));
		assert_eq!(out[1].version, CommitVersion(7));
	}
}

#[cfg(test)]
mod integration {
	use std::{collections::HashMap, thread::sleep, time::Duration as StdDuration};

	use reifydb_cdc::produce::watermark::CdcProducerWatermark;
	use reifydb_core::{
		actors::pending::PendingWrite,
		interface::WithEventBus,
		key::{Key, kind::KeyKind},
	};
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

	// Every flow now consumes every pushed batch, and batches with nothing relevant land here.
	// The threshold below is load-bearing twice over: staying in memory at or below the lag keeps
	// an idle flow from committing on every batch, and persisting beyond the lag keeps its
	// durable checkpoint moving - CDC log compaction is gated on the minimum durable checkpoint
	// across all flows, so a flow that never persisted would pin the CDC log forever.
	#[test]
	fn skip_or_checkpoint_persists_only_beyond_checkpoint_lag() {
		let te = TestEngine::builder().with_cdc().build();
		let computer = SliceComputer::new(te.inner().clone());
		let config = SliceConfig {
			chunk_size: 1000,
			checkpoint_lag: 10,
		};

		match computer.skip_or_checkpoint(FlowId(7), CommitVersion(25), CommitVersion(15), false, &config) {
			SliceStep::Skip {
				advance_to,
				more,
			} => {
				assert_eq!(advance_to, CommitVersion(25));
				assert!(!more);
			}
			_ => panic!("an advance of exactly checkpoint_lag must stay in memory, not commit"),
		}

		match computer.skip_or_checkpoint(FlowId(7), CommitVersion(26), CommitVersion(15), true, &config) {
			SliceStep::Commit {
				slice,
				advance_to,
				more,
			} => {
				assert_eq!(advance_to, CommitVersion(26));
				assert!(more);
				assert_eq!(slice.checkpoints, vec![(FlowId(7), CommitVersion(26))]);
				assert!(
					slice.combined.iter_sorted().next().is_none(),
					"a checkpoint-only slice must carry no data writes"
				);
			}
			_ => panic!("an advance beyond checkpoint_lag must persist a durable checkpoint - CDC \
				 compaction is gated on the minimum durable checkpoint across flows"),
		}
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
		let mut overlay = FlowWriteOverlay::new();

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
					&mut overlay,
				)
				.expect("step")
			{
				SliceStep::Commit {
					slice,
					advance_to,
					..
				} => {
					let (commit_version, pending) =
						committer.commit_slice(slice).expect("commit slice");
					overlay.promote(commit_version, pending);
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

	// The deferred read-skew scenario, deterministically: a slice's output rows commit at a
	// version above the chunk_end that pins the next slice's query snapshot. Owned-row keys
	// route through state_query (the lease), so a later slice must see them even with an
	// EMPTY overlay - this is exactly the post-restart window, where the in-memory overlay
	// is gone. The overlay-merged case must agree.
	#[test]
	fn pinned_slice_reads_prior_commit_across_restart_window() {
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}]");

		let engine = te.inner().clone();
		let cdc_store = engine.cdc_store();
		let flow_catalog = FlowCatalog::new(engine.catalog());

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
		let mut overlay = FlowWriteOverlay::new();

		for _ in 0..400 {
			match computer
				.step(
					&mut flow_engine,
					&cdc_store,
					SliceCursor {
						flow_id,
						source_shapes: &source_shapes,
						cursor,
						durable_cursor: cursor,
					},
					&config,
					&mut overlay,
				)
				.expect("step")
			{
				SliceStep::Commit {
					slice,
					advance_to,
					..
				} => {
					// The production interleaving: an upstream commit grabs a version
					// after the chunk was computed but before the flow output commits,
					// so the flow's own rows land above the version window the next
					// slice's query is pinned to.
					te.command("INSERT app::t [{id: 3, val: 30}]");
					let (commit_version, pending) =
						committer.commit_slice(slice).expect("commit slice");
					assert!(
						commit_version.0 > advance_to.0 + 1,
						"the slice output must commit beyond the read window pinned at chunk_end"
					);

					let row_keys: Vec<_> = pending
						.iter_sorted()
						.filter(|(k, w)| {
							matches!(Key::kind(k), Some(KeyKind::Row))
								&& matches!(w, PendingWrite::Set(_))
						})
						.map(|(k, _)| k.clone())
						.collect();
					assert!(!row_keys.is_empty(), "the slice must have produced view rows");

					overlay.promote(commit_version, pending);

					let pinned_txn = |base_pending: Arc<Pending>| {
						FlowTransaction::deferred_from_parts(DeferredParams {
							version: advance_to,
							pending: Pending::new(),
							base_pending,
							query: engine.multi().begin_query().unwrap(),
							state_query: engine.multi().begin_query().unwrap(),
							dictionary_query: None,
							single: engine.single().clone(),
							catalog: engine.catalog(),
							interceptors: engine.create_interceptors(),
							clock: engine.clock().clone(),
							allocators: flow_engine.allocators.clone(),
						})
					};

					let mut with_overlay = pinned_txn(overlay.merged());
					let mut empty_overlay = pinned_txn(Arc::new(Pending::new()));
					for key in &row_keys {
						assert!(
							empty_overlay.get(key).unwrap().is_some(),
							"restart window: a pinned txn with an empty overlay must read owned rows at the state version"
						);
						assert!(
							with_overlay.get(key).unwrap().is_some(),
							"a pinned read below the flow's commit version must see its own rows through the overlay"
						);
					}
					return;
				}
				SliceStep::Skip {
					advance_to,
					..
				} => {
					cursor = advance_to;
				}
				SliceStep::Idle => {
					sleep(StdDuration::from_millis(5));
				}
			}
		}
		panic!("no slice committed within the budget");
	}

	// Regression for the flaky `sequential_writes_materialize_exactly_via_push`. The CDC producer
	// advances its watermark on its own thread *after* commit, so `cdc_producer_watermark` can
	// transiently sit ahead of the command `done_until`. A freshly created deferred flow takes a
	// single routed Drain for its first insert; if that Drain lands inside the overshoot window the
	// old gate (`safe > done_until() -> Idle`, no reschedule) stalled the flow until the next tick,
	// which the test suppresses with FLOW_TICK=1h. `step` must instead clamp its read bound to
	// min(producer, done_until) and still process every version that is already safe (<= done_until).
	// Here we force the overshoot deterministically and assert the flow commits rather than stalls.
	#[test]
	fn step_reads_up_to_done_until_when_producer_watermark_overshoots() {
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4 } AS { FROM app::t MAP { id } }");
		te.command("INSERT app::t [{id: 1}]");

		let engine = te.inner().clone();
		let cdc_store = engine.cdc_store();
		let flow_catalog = FlowCatalog::new(engine.catalog());

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
		let config = SliceConfig {
			chunk_size: 1000,
			checkpoint_lag: 10_000,
		};

		// CDC production is async; wait until the insert's CDC is durably produced and the command
		// watermark covers it, so the version is genuinely safe to read before we force the overshoot.
		let target = engine.current_version().expect("current version");
		let producer = engine.ioc().resolve::<CdcProducerWatermark>().expect("producer watermark");
		for _ in 0..400 {
			if producer.get() >= target && engine.done_until() >= target {
				break;
			}
			sleep(StdDuration::from_millis(5));
		}
		assert!(producer.get() >= target, "CDC producer never caught up to the insert");
		assert!(engine.done_until() >= target, "command watermark never covered the insert");

		// Force the producer watermark one version ahead of `done_until` - the exact transient race.
		// `advance` only publishes contiguously, so overshoot by exactly +1 from the published value.
		producer.advance(CommitVersion(producer.get().0 + 1));
		assert!(
			producer.get() > engine.done_until(),
			"test precondition: producer watermark must overshoot done_until"
		);

		// With the clamp, `step` reads up to done_until (which covers the insert) and commits the view
		// row. Before the fix this returned `Idle` and the row was lost until the (1h) tick.
		let step = computer
			.step(
				&mut flow_engine,
				&cdc_store,
				SliceCursor {
					flow_id,
					source_shapes: &source_shapes,
					cursor: CommitVersion(0),
					durable_cursor: CommitVersion(0),
				},
				&config,
				&mut FlowWriteOverlay::new(),
			)
			.expect("step");
		match step {
			SliceStep::Commit {
				advance_to,
				..
			} => assert!(
				advance_to >= target,
				"step must advance through the insert version, got {}",
				advance_to.0
			),
			SliceStep::Idle => panic!(
				"producer overshoot must not stall the flow: step returned Idle, so the insert never \
				 materializes under a long tick"
			),
			SliceStep::Skip {
				..
			} => panic!("step skipped the insert instead of committing its view row"),
		}
	}
}
