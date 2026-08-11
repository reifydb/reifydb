// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::{Pending, PendingLayers},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::{
	engine::{
		FlowEngineInner,
		execution::{COMPLETENESS_OBJECT, frontier::WatermarkHolds},
	},
	transaction::{DeferredParams, DepFlowTransaction},
};
use reifydb_transaction::change_accumulator::ChangeAccumulator;
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime},
};

use crate::deferred::{committer::FlowSlice, overlay::FlowWriteOverlay};

pub struct SliceConfig {
	pub checkpoint_lag: u64,
}

pub struct SliceCursor<'a> {
	pub flow_id: FlowId,
	pub source_objects: &'a BTreeSet<ObjectId>,
	pub completeness_objects: Option<&'a BTreeSet<u64>>,
	pub cursor: CommitVersion,
	pub durable_cursor: CommitVersion,
}

pub enum SliceStep {
	Commit {
		slice: FlowSlice,
		advance_to: CommitVersion,
		more: bool,
		holds: WatermarkHolds,
	},

	Skip {
		advance_to: CommitVersion,
		more: bool,
		holds: WatermarkHolds,
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

	#[allow(clippy::too_many_arguments)]
	pub fn compute_pulled(
		&self,
		flow_engine: &mut FlowEngineInner,
		items: &[Arc<Cdc>],
		cursor: SliceCursor,
		advance_to: CommitVersion,
		more: bool,
		config: &SliceConfig,
		overlay: &mut FlowWriteOverlay,
	) -> Result<SliceStep> {
		overlay.prune_through(cursor.cursor);

		let start = items.partition_point(|c| c.version <= cursor.cursor);
		let refs: Vec<&Cdc> = items[start..].iter().map(Arc::as_ref).collect();
		let changes = collect_flow_changes(&refs, cursor.source_objects, cursor.completeness_objects);
		if changes.is_empty() {
			return self.skip_or_checkpoint(
				flow_engine,
				cursor.flow_id,
				advance_to,
				cursor.durable_cursor,
				more,
				config,
			);
		}

		overlay.prune_through(advance_to);
		let (combined, view_changes, holds) =
			self.compute(flow_engine, cursor.flow_id, advance_to, changes, overlay.merged())?;

		Ok(SliceStep::Commit {
			slice: FlowSlice {
				combined,
				checkpoints: vec![(cursor.flow_id, advance_to)],
				checkpoint_deletes: Vec::new(),
				view_changes,
				control_cursor: None,
			},
			advance_to,
			more,
			holds,
		})
	}

	fn skip_or_checkpoint(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		advance_to: CommitVersion,
		durable_cursor: CommitVersion,
		more: bool,
		config: &SliceConfig,
	) -> Result<SliceStep> {
		let holds = self.resolved_holds(flow_engine, flow_id, advance_to)?;
		if advance_to.0.saturating_sub(durable_cursor.0) > config.checkpoint_lag {
			let mut slice = FlowSlice::empty();
			slice.checkpoints.push((flow_id, advance_to));
			return Ok(SliceStep::Commit {
				slice,
				advance_to,
				more,
				holds,
			});
		}
		Ok(SliceStep::Skip {
			advance_to,
			more,
			holds,
		})
	}

	pub(crate) fn resolved_holds(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		state_version: CommitVersion,
	) -> Result<WatermarkHolds> {
		let catalog: Catalog = self.engine.catalog();
		let interceptors = self.engine.create_interceptors();

		let (_current, state_lease) = self.engine.acquire_current_snapshot_lease()?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&state_lease)?;

		let mut query = base_query;
		query.read_as_of_version_inclusive(state_version);

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending: PendingLayers::empty(),
			query,
			state_query,
			single: self.engine.single().clone(),
			catalog,
			interceptors,
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.fold_published_arrivals(&mut txn, flow_id, state_version)?;
		flow_engine.holds(&mut txn, flow_id)
	}

	fn compute(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		state_version: CommitVersion,
		changes: Vec<Change>,
		pending: PendingLayers,
	) -> Result<(Pending, Vec<Change>, WatermarkHolds)> {
		let catalog: Catalog = self.engine.catalog();
		let interceptors = self.engine.create_interceptors();

		let (_current, state_lease) = self.engine.acquire_current_snapshot_lease()?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&state_lease)?;

		let mut query = base_query;
		query.read_as_of_version_inclusive(state_version);

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending,
			query,
			state_query,
			single: self.engine.single().clone(),
			catalog,
			interceptors,
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.process_batch(&mut txn, changes, flow_id)?;
		let holds = flow_engine.holds(&mut txn, flow_id)?;
		txn.flush_operator_states()?;

		let view_changes = self.consolidated_view_changes(&mut txn, state_version)?;

		let pending = txn.take_pending();
		Ok((pending, view_changes, holds))
	}

	fn consolidated_view_changes(
		&self,
		txn: &mut DepFlowTransaction,
		state_version: CommitVersion,
	) -> Result<Vec<Change>> {
		let mut accumulator = ChangeAccumulator::new();
		for (id, diff) in txn.take_accumulator_entries() {
			accumulator.track(id, diff);
		}
		accumulator.take_changes(state_version, self.engine.clock().now())
	}

	pub fn tick(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		timestamp: DateTime,
		checkpoint: CommitVersion,
	) -> Result<(Pending, Vec<Change>)> {
		let (state_version, lease) = self.engine.acquire_current_snapshot_lease()?;
		let query = self.engine.multi().begin_query_at_version(&lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&lease)?;

		let mut txn = DepFlowTransaction::deferred_from_parts(DeferredParams {
			version: state_version,
			pending: PendingLayers::empty(),
			query,
			state_query,
			single: self.engine.single().clone(),
			catalog: self.engine.catalog(),
			interceptors: self.engine.create_interceptors(),
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.process_tick(&mut txn, flow_id, timestamp, checkpoint)?;
		txn.flush_operator_states()?;

		let view_changes = self.consolidated_view_changes(&mut txn, state_version)?;
		Ok((txn.take_pending(), view_changes))
	}
}

pub(crate) fn collect_flow_changes(
	cdcs: &[&Cdc],
	source_objects: &BTreeSet<ObjectId>,
	completeness_objects: Option<&BTreeSet<u64>>,
) -> Vec<Change> {
	let mut out = Vec::new();
	for cdc in cdcs {
		for change in &cdc.changes {
			let relevant = match change.origin {
				ChangeOrigin::Object(object) if object == COMPLETENESS_OBJECT => {
					completeness_wakes(change, completeness_objects)
				}
				ChangeOrigin::Object(object) => source_objects.contains(&object),
				ChangeOrigin::Flow(_) => true,
			};
			if relevant {
				out.push(change.clone());
			}
		}
	}
	out
}

fn completeness_wakes(change: &Change, completeness_objects: Option<&BTreeSet<u64>>) -> bool {
	let Some(admitted) = completeness_objects else {
		return true;
	};
	change.diffs.iter().filter_map(|diff| diff.post()).any(|columns| {
		let Some(objects) = columns.column("object_id") else {
			return false;
		};
		(0..columns.row_count()).any(|row| match objects.data().get_value(row) {
			Value::Uint8(object) => admitted.contains(&object),
			_ => false,
		})
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			catalog::{
				flow::OperatorId,
				id::{TableId, ViewId},
			},
			change::Diff,
		},
		value::column::columns::Columns,
	};
	use smallvec::smallvec;

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
	fn object_changes_match_source_objects() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(
			5,
			vec![
				change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 5),
				change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5),
				change(ChangeOrigin::Object(ObjectId::View(ViewId(9))), 5),
			],
		)];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Object(ObjectId::Table(TableId(1)))));
	}

	#[test]
	fn flow_origin_changes_always_included() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(5, vec![change(ChangeOrigin::Flow(OperatorId(42)), 5)])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Flow(OperatorId(42))));
	}

	#[test]
	fn unrelated_object_changes_excluded() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5)]),
			cdc(6, vec![change(ChangeOrigin::Object(ObjectId::View(ViewId(3))), 6)]),
		];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert!(out.is_empty());
	}

	#[test]
	fn completeness_changes_survive_a_source_set_that_excludes_them() {
		// In no flow's source set, so without the carve-out the slice is empty and short-circuits.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		assert!(
			!sources.contains(&COMPLETENESS_OBJECT),
			"the fixture must exclude the completeness table or nothing is proven"
		);
		let cdcs = vec![cdc(
			5,
			vec![
				change(ChangeOrigin::Object(COMPLETENESS_OBJECT), 5),
				change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5),
			],
		)];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert!(
			out.iter().any(|c| matches!(c.origin, ChangeOrigin::Object(o) if o == COMPLETENESS_OBJECT)),
			"the completeness change must survive a source set that excludes it"
		);
		assert_eq!(out.len(), 1, "the unrelated table must still be filtered out");
	}

	#[test]
	fn changes_gathered_across_multiple_cdc_entries_in_order() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![
			cdc(5, vec![change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 5)]),
			cdc(7, vec![change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 7)]),
		];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert_eq!(out.len(), 2);
		assert_eq!(out[0].version, CommitVersion(5));
		assert_eq!(out[1].version, CommitVersion(7));
	}

	fn completeness_change(objects: &[u64]) -> Change {
		Change {
			origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
			version: CommitVersion(5),
			diffs: smallvec![Diff::insert(Columns::from_rows(
				&["object_id", "complete_through"],
				&objects.iter()
					.map(|o| vec![Value::Uint8(*o), Value::DateTime(DateTime::default())])
					.collect::<Vec<_>>(),
			))],
			changed_at: DateTime::default(),
		}
	}

	#[test]
	fn an_assertion_outside_the_upstream_closure_does_not_wake_the_flow() {
		// Nothing upstream of the flow moved, so waking it commits a slice that can fold nothing.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [1].into_iter().collect();
		let cdcs = vec![cdc(5, vec![completeness_change(&[77])])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, Some(&admitted));

		assert!(out.is_empty(), "an assertion by an unrelated object must leave the slice empty");
	}

	#[test]
	fn an_assertion_inside_the_upstream_closure_still_wakes_the_flow() {
		// The change is the wake signal, so dropping this one strands the flow with no way to recompute.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [1].into_iter().collect();
		let cdcs = vec![cdc(5, vec![completeness_change(&[1])])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, Some(&admitted));

		assert_eq!(out.len(), 1, "an ancestor's assertion must survive the filter");
	}

	#[test]
	fn one_admitted_object_among_many_wakes_the_flow() {
		// Assertions batch several objects into one row set; requiring all of them would drop the wake.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [3].into_iter().collect();
		let cdcs = vec![cdc(5, vec![completeness_change(&[77, 88, 3, 99])])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, Some(&admitted));

		assert_eq!(out.len(), 1, "a single ancestor among unrelated assertions must still wake the flow");
	}

	#[test]
	fn an_absent_admission_set_keeps_the_unconditional_wake() {
		// A flow whose closure could not be resolved must degrade to waking on every assertion, never to
		// silence.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let cdcs = vec![cdc(5, vec![completeness_change(&[77])])];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, None);

		assert_eq!(out.len(), 1, "no admission set must mean admit, not reject");
	}

	#[test]
	fn a_retracted_assertion_carries_no_post_image_and_wakes_nothing() {
		// A delete has no post image to read an id from, and admitting it blindly would restore the full
		// fan-out.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [1].into_iter().collect();
		let pre = Columns::from_rows(
			&["object_id", "complete_through"],
			&[vec![Value::Uint8(1), Value::DateTime(DateTime::default())]],
		);
		let cdcs = vec![cdc(
			5,
			vec![Change {
				origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
				version: CommitVersion(5),
				diffs: smallvec![Diff::remove(pre)],
				changed_at: DateTime::default(),
			}],
		)];

		let out = collect_flow_changes(&cdcs.iter().collect::<Vec<_>>(), &sources, Some(&admitted));

		assert!(out.is_empty(), "a retraction asserts nothing and must not wake the flow");
	}
}

#[cfg(test)]
mod integration {
	use std::{collections::HashSet, ops::Bound, thread::sleep, time::Duration as StdDuration};

	use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::TimeDomain,
		interface::{
			WithEventBus,
			catalog::{
				flow::OperatorId,
				id::{SeriesId, TableId, ViewId},
			},
		},
		key::{Key, kind::KeyKind},
	};
	use reifydb_flow::{
		engine::execution::frontier::WatermarkHold,
		operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
		transaction::{read::ReadFrom, substrate::FlowSubstrate},
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		operator::{FlowEdge, FlowNode, OperatorDef},
	};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{factory::time::at_millis, util::cowvec::CowVec, value::identity::IdentityId};

	use super::*;
	use crate::{
		catalog::FlowCatalog,
		deferred::{
			committer::Committer, quiescence::FlowMaterialization, routing, tracker::FlowPositionTracker,
		},
	};

	fn view_row_count(te: &TestEngine, rql: &str) -> usize {
		te.query(rql).first().map(|f| f.row_count()).unwrap_or(0)
	}

	#[allow(clippy::too_many_arguments)]
	fn pull_step(
		engine: &StandardEngine,
		computer: &SliceComputer,
		flow_engine: &mut FlowEngineInner,
		cursor: SliceCursor,
		config: &SliceConfig,
		overlay: &mut FlowWriteOverlay,
	) -> Option<SliceStep> {
		// The actor's drain path in miniature; None stands in for its "nothing to do" return.
		let safe = engine.cdc_producer_watermark().min(engine.done_until());
		if safe <= cursor.cursor {
			return None;
		}
		let batch = engine
			.cdc_store()
			.read_range(Bound::Excluded(cursor.cursor), Bound::Included(safe), 1000)
			.expect("read cdc range");
		let more = batch.has_more;
		let items: Vec<Arc<Cdc>> = batch.items.into_iter().map(Arc::new).collect();
		let advance_to = if more {
			items.last().expect("has_more implies items").version
		} else {
			safe
		};
		Some(computer
			.compute_pulled(flow_engine, &items, cursor, advance_to, more, config, overlay)
			.expect("compute_pulled"))
	}

	fn build_flow_engine(engine: &StandardEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			engine.event_bus().clone(),
			RuntimeContext::with_clock(engine.clock().clone()),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state()),
			OperatorSampleRegistry::new(),
		)
	}

	fn seeding_txn(
		engine: &StandardEngine,
		flow_engine: &FlowEngineInner,
		version: CommitVersion,
	) -> DepFlowTransaction {
		let (_current, lease) = engine.acquire_current_snapshot_lease().unwrap();
		let mut query = engine.multi().begin_query_at_version(&lease).unwrap();
		let state_query = engine.multi().begin_query_at_version(&lease).unwrap();
		query.read_as_of_version_inclusive(version);

		DepFlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: PendingLayers::empty(),
			query,
			state_query,
			single: engine.single().clone(),
			catalog: engine.catalog(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		})
	}

	#[test]
	fn a_quiet_flow_still_holds_a_frontier_from_state_restored_before_any_change_arrives() {
		// A producer with no incoming rows must still claim its frontier at startup, or its consumer stays
		// pinned at the epoch forever.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let mut flow_engine = build_flow_engine(&engine);
		let flow = FlowId(1);
		let version = CommitVersion(5);

		let mut builder = FlowDag::builder(flow);
		builder.add_node(FlowNode::new(
			OperatorId(1),
			OperatorDef::SourceSeries {
				series: SeriesId(1),
				time_domain: TimeDomain::Event,
			},
		));
		builder.add_node(FlowNode::new(
			OperatorId(3),
			OperatorDef::SinkTableView {
				view: ViewId(3),
				table: TableId(3),
			},
		));
		builder.add_edge(FlowEdge::new(1, OperatorId(1), OperatorId(3))).unwrap();
		flow_engine.register_flow_dag(builder.build());
		flow_engine.add_sink(flow, OperatorId(3), ObjectId::View(ViewId(3)));

		let mut seed = seeding_txn(&engine, &flow_engine, version);
		let watermarks = seed.source_watermarks();
		watermarks.advance(OperatorId(1), &mut seed, at_millis(30_000)).unwrap();
		seed.flush_operator_states().unwrap();

		let computer = SliceComputer::new(engine.clone());
		let held = computer.resolved_holds(&mut flow_engine, flow, version).unwrap();

		assert_eq!(
			held,
			vec![WatermarkHold {
				object: ObjectId::View(ViewId(3)),
				frontier: at_millis(30_000)
			}],
			"resolved_holds must read the seeded watermark through the shared substrate"
		);
	}

	#[test]
	fn skip_or_checkpoint_persists_only_beyond_checkpoint_lag() {
		// The threshold is load-bearing twice over: at or below the lag an idle flow must not
		// commit on every batch, and beyond it its durable checkpoint must move, because CDC
		// compaction is gated on the minimum durable checkpoint across flows.
		let te = TestEngine::builder().with_cdc().build();
		let engine = te.inner().clone();
		let mut flow_engine = build_flow_engine(&engine);
		let computer = SliceComputer::new(engine.clone());
		let config = SliceConfig {
			checkpoint_lag: 10,
		};

		match computer
			.skip_or_checkpoint(
				&mut flow_engine,
				FlowId(7),
				CommitVersion(25),
				CommitVersion(15),
				false,
				&config,
			)
			.unwrap()
		{
			SliceStep::Skip {
				advance_to,
				more,
				holds: _,
			} => {
				assert_eq!(advance_to, CommitVersion(25));
				assert!(!more);
			}
			_ => panic!("an advance of exactly checkpoint_lag must stay in memory, not commit"),
		}

		match computer
			.skip_or_checkpoint(
				&mut flow_engine,
				FlowId(7),
				CommitVersion(26),
				CommitVersion(15),
				true,
				&config,
			)
			.unwrap()
		{
			SliceStep::Commit {
				slice,
				advance_to,
				more,
				holds: _,
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
	fn a_step_with_nothing_to_do_still_drains_generations_at_or_below_the_cursor() {
		// A generation at or below the cursor is already served by the store at any version a
		// later compute can pin, so a step that decides to do nothing must still drop it -
		// otherwise an idle flow keeps every write set it has ever committed.
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.command("INSERT app::t [{id: 1, val: 10}]");

		let engine = te.inner().clone();
		let mut flow_engine = build_flow_engine(&engine);

		// An empty source set makes every CDC record irrelevant, reproducing a caught-up flow
		// with nothing it cares about being written.
		let source_objects: BTreeSet<ObjectId> = BTreeSet::new();
		let computer = SliceComputer::new(engine.clone());
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut overlay = FlowWriteOverlay::new();

		te.await_cdc();

		let mut drive = |cursor: &mut CommitVersion, overlay: &mut FlowWriteOverlay| {
			for _ in 0..400 {
				match pull_step(
					&engine,
					&computer,
					&mut flow_engine,
					SliceCursor {
						flow_id: FlowId(1),
						source_objects: &source_objects,
						completeness_objects: None,
						cursor: *cursor,
						durable_cursor: CommitVersion(0),
					},
					&config,
					overlay,
				) {
					Some(SliceStep::Commit {
						advance_to,
						..
					})
					| Some(SliceStep::Skip {
						advance_to,
						..
					}) => *cursor = advance_to,
					None => return,
				}
			}
			panic!("the drive loop never settled");
		};

		// Promoting AT the settled cursor is the steady state that leaks: a caught-up flow whose
		// own commit has just been promoted.
		let mut cursor = CommitVersion(0);
		drive(&mut cursor, &mut overlay);
		let mut pending = Pending::new();
		pending.insert(EncodedKey::new(b"own-write"), EncodedBytes(CowVec::new(vec![1, 2, 3])));
		overlay.promote(cursor, pending);
		assert_eq!(overlay.generations_len(), 1, "precondition: one unpruned write set");

		// A commit this flow does not care about: the step it triggers takes the
		// nothing-relevant exit, and that exact step must be the one that prunes.
		te.command("INSERT app::t [{id: 2, val: 20}]");
		te.await_cdc();
		let before = cursor;
		drive(&mut cursor, &mut overlay);
		assert!(
			cursor > before,
			"precondition: the irrelevant commit must have advanced the cursor, or no step ran"
		);
		assert_eq!(
			overlay.generations_len(),
			0,
			"a generation at or below the cursor must be dropped even by a step that does no work"
		);
	}

	#[test]
	fn deferred_view_materializes_through_slice_step() {
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}, {id: 3, val: 30}]");

		let engine = te.inner().clone();
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

		let source_objects = {
			let graph = flow_engine.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_route = |vid| {
				flow_catalog.find_view(vid).map(|v| routing::ViewRoute {
					kind: v.kind(),
					storage: v.storage_id(),
				})
			};
			routing::flow_source_objects(&graph, flow_id, &registered, &view_route)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = Committer::new(
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut durable = CommitVersion(0);
		let mut committed_any = false;
		let mut overlay = FlowWriteOverlay::new();

		// CDC production is async, so the drain has to spin until the producer catches up.
		for _ in 0..400 {
			match pull_step(
				&engine,
				&computer,
				&mut flow_engine,
				SliceCursor {
					flow_id,
					source_objects: &source_objects,
					completeness_objects: None,
					cursor,
					durable_cursor: durable,
				},
				&config,
				&mut overlay,
			) {
				Some(SliceStep::Commit {
					slice,
					advance_to,
					..
				}) => {
					let (commit_version, pending) =
						committer.commit_slice(&engine, slice).expect("commit slice");
					overlay.promote(commit_version, pending);
					cursor = advance_to;
					durable = advance_to;
					committed_any = true;
				}
				Some(SliceStep::Skip {
					advance_to,
					..
				}) => {
					cursor = advance_to;
				}
				None => {
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

	#[test]
	fn pinned_slice_reads_prior_commit_across_restart_window() {
		// A slice's output rows commit above the chunk_end pinning the next slice's snapshot, so
		// a later slice must still see them with an EMPTY overlay - the post-restart window,
		// where the in-memory overlay is gone.
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}]");

		let engine = te.inner().clone();
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

		let source_objects = {
			let graph = flow_engine.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_route = |vid| {
				flow_catalog.find_view(vid).map(|v| routing::ViewRoute {
					kind: v.kind(),
					storage: v.storage_id(),
				})
			};
			routing::flow_source_objects(&graph, flow_id, &registered, &view_route)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = Committer::new(
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut overlay = FlowWriteOverlay::new();

		for _ in 0..400 {
			match pull_step(
				&engine,
				&computer,
				&mut flow_engine,
				SliceCursor {
					flow_id,
					source_objects: &source_objects,
					completeness_objects: None,
					cursor,
					durable_cursor: cursor,
				},
				&config,
				&mut overlay,
			) {
				Some(SliceStep::Commit {
					slice,
					advance_to,
					..
				}) => {
					// An upstream commit grabs a version after the chunk was computed
					// but before the flow output commits, so the flow's own rows land
					// above the window the next slice is pinned to.
					te.command("INSERT app::t [{id: 3, val: 30}]");
					let (commit_version, pending) =
						committer.commit_slice(&engine, slice).expect("commit slice");
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

					let pinned_txn = |pending: PendingLayers| {
						DepFlowTransaction::deferred_from_parts(DeferredParams {
							version: advance_to,
							pending,
							query: engine.multi().begin_query().unwrap(),
							state_query: engine.multi().begin_query().unwrap(),
							single: engine.single().clone(),
							catalog: engine.catalog(),
							interceptors: engine.create_interceptors(),
							clock: engine.clock().clone(),
							substrate: flow_engine.substrate().clone(),
						})
					};

					let mut with_overlay = pinned_txn(overlay.merged());
					let mut empty_overlay = pinned_txn(PendingLayers::empty());
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
				Some(SliceStep::Skip {
					advance_to,
					..
				}) => {
					cursor = advance_to;
				}
				None => {
					sleep(StdDuration::from_millis(5));
				}
			}
		}
		panic!("no slice committed within the budget");
	}

	#[test]
	fn a_flow_never_commits_a_key_it_would_later_read_through_the_pinned_query() {
		// A Query-routed write is the only class that could read stale below its own commit
		// version, so it is the only class the overlay could be load-bearing for. If a flow never
		// commits one, the overlay is not needed at all.
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
		te.admin("CREATE DEFERRED VIEW app::v { g: int4, total: int8 } \
			 AS { FROM app::t AGGREGATE { total: math::count(id) } BY { g } }");
		te.command(
			r#"INSERT app::t [{id: 1, g: 1, ts: "1970-01-01T00:00:00Z"},
			                   {id: 2, g: 1, ts: "1970-01-01T00:01:00Z"},
			                   {id: 3, g: 2, ts: "1970-01-01T00:02:00Z"}]"#,
		);

		let engine = te.inner().clone();
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

		let source_objects = {
			let graph = flow_engine.get_dependency_graph();
			let registered = |f: FlowId| f == flow_id;
			let view_route = |vid| {
				flow_catalog.find_view(vid).map(|v| routing::ViewRoute {
					kind: v.kind(),
					storage: v.storage_id(),
				})
			};
			routing::flow_source_objects(&graph, flow_id, &registered, &view_route)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = Committer::new(
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut durable = CommitVersion(0);
		let mut overlay = FlowWriteOverlay::new();
		let mut committed_kinds: HashSet<Option<KeyKind>> = HashSet::new();
		let mut stale_reads: HashSet<Option<KeyKind>> = HashSet::new();

		for _ in 0..400 {
			match pull_step(
				&engine,
				&computer,
				&mut flow_engine,
				SliceCursor {
					flow_id,
					source_objects: &source_objects,
					completeness_objects: None,
					cursor,
					durable_cursor: durable,
				},
				&config,
				&mut overlay,
			) {
				Some(SliceStep::Commit {
					slice,
					advance_to,
					..
				}) => {
					let (commit_version, pending) =
						committer.commit_slice(&engine, slice).expect("commit slice");
					let mut live_keys = Vec::new();
					for (key, write) in pending.iter_sorted() {
						committed_kinds.insert(Key::kind(key));
						if DepFlowTransaction::read_from(key) == ReadFrom::Query {
							stale_reads.insert(Key::kind(key));
						}
						if matches!(write, PendingWrite::Set(_)) {
							live_keys.push(key.clone());
						}
					}
					overlay.promote(commit_version, pending);

					// The restart window asserted directly rather than by inference, and
					// it reaches OperatorState as well as Row.
					let mut empty_overlay =
						DepFlowTransaction::deferred_from_parts(DeferredParams {
							version: advance_to,
							pending: PendingLayers::empty(),
							query: engine.multi().begin_query().unwrap(),
							state_query: engine.multi().begin_query().unwrap(),
							single: engine.single().clone(),
							catalog: engine.catalog(),
							interceptors: engine.create_interceptors(),
							clock: engine.clock().clone(),
							substrate: flow_engine.substrate().clone(),
						});
					for key in &live_keys {
						assert!(
							empty_overlay.get(key).unwrap().is_some(),
							"restart window: {:?} must resolve with no overlay at all",
							Key::kind(key)
						);
					}

					cursor = advance_to;
					durable = advance_to;
				}
				Some(SliceStep::Skip {
					advance_to,
					..
				}) => {
					cursor = advance_to;
				}
				None => {
					if view_row_count(&te, "FROM app::v") == 2 {
						break;
					}
					sleep(StdDuration::from_millis(5));
				}
			}
		}

		assert_eq!(view_row_count(&te, "FROM app::v"), 2, "the aggregate never materialized its two groups");
		// Without both classes present the routing assertion below would pass vacuously; an
		// aggregate is used because it writes operator state as well as view rows.
		assert!(
			committed_kinds.contains(&Some(KeyKind::OperatorState)),
			"expected the aggregate to commit operator state, saw only {committed_kinds:?}"
		);
		assert!(
			committed_kinds.contains(&Some(KeyKind::Row)),
			"expected the aggregate to commit view rows, saw only {committed_kinds:?}"
		);

		assert!(
			stale_reads.is_empty(),
			"a flow committed keys that it would read back through the version-pinned query: {stale_reads:?}. \
			 Those reads cannot see the flow's own commit, so FlowWriteOverlay is load-bearing for them and \
			 must not be removed"
		);
	}
}
