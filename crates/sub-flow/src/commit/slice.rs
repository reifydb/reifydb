// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_cdc::rebuild::{changed_objects, rebuild_selected_changes};
use reifydb_core::{
	actors::pending::Pending,
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowId, object::ObjectId},
		cdc::Cdc,
		change::{Change, ChangeOrigin},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_flow::{
	engine::{COMPLETENESS_OBJECT, FlowEngineInner, frontier::WatermarkHolds},
	transaction::{DeferredParams, FlowTransaction, deferred::DeferredTransaction},
};
use reifydb_transaction::{accumulator::ChangeAccumulator, transaction::Transaction};
use reifydb_value::{
	Result,
	value::{Value, identity::IdentityId},
};

use crate::commit::committer::FlowSlice;

pub struct SliceConfig {
	pub checkpoint_lag: u64,
}

pub struct SliceCursor<'a> {
	pub flow_id: FlowId,
	pub source_objects: &'a BTreeSet<ObjectId>,
	pub completeness_objects: Option<&'a BTreeSet<u64>>,
	pub cursor: CommitVersion,
	pub durable_cursor: CommitVersion,
	pub has_readers: bool,
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
	) -> Result<SliceStep> {
		let start = items.partition_point(|c| c.version.commit <= cursor.cursor);
		let (mut advance_to, mut more) = (advance_to, more);
		let mut relevant: Vec<&Cdc> = Vec::new();
		let cut_per_source = cursor.has_readers || cursor.source_objects.len() > 1;
		for cdc in items[start..].iter().map(Arc::as_ref) {
			if !is_relevant(cdc, cursor.source_objects) {
				continue;
			}
			if let Some(first) = relevant.first()
				&& cdc.version.source != first.version.source
				&& cut_per_source
			{
				advance_to = CommitVersion(cdc.version.source.0 - 1);
				more = true;
				break;
			}
			relevant.push(cdc);
		}
		let source = relevant.iter().map(|cdc| cdc.version.source).max();
		let changes = collect_flow_changes(
			&self.engine,
			&relevant,
			cursor.source_objects,
			cursor.completeness_objects,
		)?;
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

		let (combined, view_changes, holds) = self.compute(flow_engine, cursor.flow_id, advance_to, changes)?;
		if combined.is_empty()
			&& view_changes.is_empty()
			&& !checkpoint_due(advance_to, cursor.durable_cursor, config)
		{
			return Ok(SliceStep::Skip {
				advance_to,
				more,
				holds,
			});
		}

		Ok(SliceStep::Commit {
			slice: FlowSlice {
				combined,
				checkpoints: vec![(cursor.flow_id, advance_to)],
				checkpoint_deletes: Vec::new(),
				view_changes,
				control_cursor: None,
				source,
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
		let (holds, folded) = self.resolved_holds(flow_engine, flow_id, advance_to)?;
		if folded.is_empty() && !checkpoint_due(advance_to, durable_cursor, config) {
			return Ok(SliceStep::Skip {
				advance_to,
				more,
				holds,
			});
		}
		let mut slice = FlowSlice::empty();
		slice.combined = folded;
		slice.checkpoints.push((flow_id, advance_to));
		Ok(SliceStep::Commit {
			slice,
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
	) -> Result<(WatermarkHolds, Pending)> {
		let catalog: Catalog = self.engine.catalog();
		let interceptors = self.engine.create_interceptors();

		let mut txn = DeferredTransaction::new(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query: None,
			state_query: None,
			catalog,
			interceptors,
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.fold_published_arrivals(&mut txn, flow_id, state_version)?;
		let holds = flow_engine.holds(&mut txn, flow_id)?;
		Ok((holds, txn.take_pending()))
	}

	fn compute(
		&self,
		flow_engine: &mut FlowEngineInner,
		flow_id: FlowId,
		state_version: CommitVersion,
		changes: Vec<Change>,
	) -> Result<(Pending, Vec<Change>, WatermarkHolds)> {
		let catalog: Catalog = self.engine.catalog();
		let interceptors = self.engine.create_interceptors();

		let (_current, state_lease) = self.engine.acquire_current_snapshot_lease()?;
		let base_query = self.engine.multi().begin_query_at_version(&state_lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&state_lease)?;

		let mut query = base_query;
		query.read_as_of_version_inclusive(state_version);

		let mut txn = DeferredTransaction::new(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query: Some(query),
			state_query: Some(state_query),
			catalog,
			interceptors,
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.process_batch(&mut txn, changes, flow_id)?;
		let holds = flow_engine.holds(&mut txn, flow_id)?;

		let view_changes = self.consolidated_view_changes(&mut txn, state_version)?;

		let pending = txn.take_pending();
		Ok((pending, view_changes, holds))
	}

	fn consolidated_view_changes(
		&self,
		txn: &mut DeferredTransaction,
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
		checkpoint: CommitVersion,
	) -> Result<(Pending, Vec<Change>)> {
		let (state_version, lease) = self.engine.acquire_current_snapshot_lease()?;
		let query = self.engine.multi().begin_query_at_version(&lease)?;
		let state_query = self.engine.multi().begin_query_at_version(&lease)?;

		let mut txn = DeferredTransaction::new(DeferredParams {
			version: state_version,
			pending: Pending::new(),
			query: Some(query),
			state_query: Some(state_query),
			catalog: self.engine.catalog(),
			interceptors: self.engine.create_interceptors(),
			clock: self.engine.clock().clone(),
			substrate: flow_engine.substrate().clone(),
		});

		flow_engine.process_tick(&mut txn, flow_id, checkpoint)?;

		let view_changes = self.consolidated_view_changes(&mut txn, state_version)?;
		Ok((txn.take_pending(), view_changes))
	}
}

fn checkpoint_due(advance_to: CommitVersion, durable_cursor: CommitVersion, config: &SliceConfig) -> bool {
	advance_to.0.saturating_sub(durable_cursor.0) > config.checkpoint_lag
}

fn accepts(object: ObjectId, source_objects: &BTreeSet<ObjectId>) -> bool {
	object == COMPLETENESS_OBJECT || source_objects.contains(&object)
}

fn is_relevant(cdc: &Cdc, source_objects: &BTreeSet<ObjectId>) -> bool {
	changed_objects(cdc).into_iter().any(|object| accepts(object, source_objects))
}

pub(crate) fn collect_flow_changes(
	engine: &StandardEngine,
	relevant: &[&Cdc],
	source_objects: &BTreeSet<ObjectId>,
	completeness_objects: Option<&BTreeSet<u64>>,
) -> Result<Vec<Change>> {
	if relevant.is_empty() {
		return Ok(Vec::new());
	}

	let catalog = engine.catalog();
	let mut query = engine.begin_query(IdentityId::system())?;
	let mut txn = Transaction::Query(&mut query);

	let mut out = Vec::new();
	for cdc in relevant {
		let rebuilt =
			rebuild_selected_changes(cdc, &catalog, &mut txn, |object| accepts(object, source_objects))?;
		out.extend(retain_relevant(rebuilt, source_objects, completeness_objects));
	}
	Ok(out)
}

pub(crate) fn retain_relevant(
	changes: Vec<Change>,
	source_objects: &BTreeSet<ObjectId>,
	completeness_objects: Option<&BTreeSet<u64>>,
) -> Vec<Change> {
	changes.into_iter()
		.filter(|change| match change.origin {
			ChangeOrigin::Object(object) if object == COMPLETENESS_OBJECT => {
				completeness_wakes(change, completeness_objects)
			}
			ChangeOrigin::Object(object) => source_objects.contains(&object),
			ChangeOrigin::Flow(_) => true,
		})
		.collect()
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
		common::ChangeVersion,
		interface::{
			catalog::{
				flow::OperatorId,
				id::{TableId, ViewId},
			},
			change::Diff,
		},
		value::column::columns::Columns,
	};
	use reifydb_value::value::datetime::DateTime;
	use smallvec::smallvec;

	use super::*;

	fn change(origin: ChangeOrigin, version: u64) -> Change {
		Change {
			origin,
			version: ChangeVersion::from(CommitVersion(version)),
			diffs: smallvec![Diff::Insert {
				post: Columns::empty(),
				origin: None,
			}],
			changed_at: DateTime::default(),
		}
	}

	#[test]
	fn object_changes_match_source_objects() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let rebuilt = vec![
			change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 5),
			change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5),
			change(ChangeOrigin::Object(ObjectId::View(ViewId(9))), 5),
		];

		let out = retain_relevant(rebuilt, &sources, None);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Object(ObjectId::Table(TableId(1)))));
	}

	#[test]
	fn flow_origin_changes_always_included() {
		// The rebuild emits object origins only, so a flow origin must never be dropped by the filter.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();

		let out = retain_relevant(vec![change(ChangeOrigin::Flow(OperatorId(42)), 5)], &sources, None);

		assert_eq!(out.len(), 1);
		assert!(matches!(out[0].origin, ChangeOrigin::Flow(OperatorId(42))));
	}

	#[test]
	fn unrelated_object_changes_excluded() {
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let rebuilt = vec![
			change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5),
			change(ChangeOrigin::Object(ObjectId::View(ViewId(3))), 6),
		];

		let out = retain_relevant(rebuilt, &sources, None);

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
		let rebuilt = vec![
			change(ChangeOrigin::Object(COMPLETENESS_OBJECT), 5),
			change(ChangeOrigin::Object(ObjectId::Table(TableId(2))), 5),
		];

		let out = retain_relevant(rebuilt, &sources, None);

		assert!(
			out.iter().any(|c| matches!(c.origin, ChangeOrigin::Object(o) if o == COMPLETENESS_OBJECT)),
			"the completeness change must survive a source set that excludes it"
		);
		assert_eq!(out.len(), 1, "the unrelated table must still be filtered out");
	}

	#[test]
	fn changes_gathered_across_multiple_cdc_entries_in_order() {
		// collect_flow_changes concatenates per-record output, so the filter must preserve order.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let rebuilt = vec![
			change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 5),
			change(ChangeOrigin::Object(ObjectId::Table(TableId(1))), 7),
		];

		let out = retain_relevant(rebuilt, &sources, None);

		assert_eq!(out.len(), 2);
		assert_eq!(out[0].version, ChangeVersion::from(CommitVersion(5)));
		assert_eq!(out[1].version, ChangeVersion::from(CommitVersion(7)));
	}

	fn completeness_change(objects: &[u64]) -> Change {
		Change {
			origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
			version: ChangeVersion::from(CommitVersion(5)),
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

		let out = retain_relevant(vec![completeness_change(&[77])], &sources, Some(&admitted));

		assert!(out.is_empty(), "an assertion by an unrelated object must leave the slice empty");
	}

	#[test]
	fn an_assertion_inside_the_upstream_closure_still_wakes_the_flow() {
		// The change is the wake signal, so dropping this one strands the flow with no way to recompute.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [1].into_iter().collect();

		let out = retain_relevant(vec![completeness_change(&[1])], &sources, Some(&admitted));

		assert_eq!(out.len(), 1, "an ancestor's assertion must survive the filter");
	}

	#[test]
	fn one_admitted_object_among_many_wakes_the_flow() {
		// Assertions batch several objects into one row set; requiring all of them would drop the wake.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();
		let admitted: BTreeSet<u64> = [3].into_iter().collect();

		let out = retain_relevant(vec![completeness_change(&[77, 88, 3, 99])], &sources, Some(&admitted));

		assert_eq!(out.len(), 1, "a single ancestor among unrelated assertions must still wake the flow");
	}

	#[test]
	fn an_absent_admission_set_keeps_the_unconditional_wake() {
		// A flow whose closure could not be resolved must degrade to waking on every assertion, never to
		// silence.
		let sources: BTreeSet<ObjectId> = [ObjectId::Table(TableId(1))].into_iter().collect();

		let out = retain_relevant(vec![completeness_change(&[77])], &sources, None);

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
		let retraction = Change {
			origin: ChangeOrigin::Object(COMPLETENESS_OBJECT),
			version: ChangeVersion::from(CommitVersion(5)),
			diffs: smallvec![Diff::remove(pre)],
			changed_at: DateTime::default(),
		};

		let out = retain_relevant(vec![retraction], &sources, Some(&admitted));

		assert!(out.is_empty(), "a retraction asserts nothing and must not wake the flow");
	}
}

#[cfg(test)]
mod integration {
	use std::{collections::HashSet, ops::Bound, sync::mpsc, thread::sleep, time::Duration as StdDuration};

	use reifydb_cdc::consume::watermark::CdcConsumerWatermark;
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::TimeDomain,
		interface::catalog::{
			flow::OperatorId,
			id::{SeriesId, TableId, ViewId},
		},
		key::tag::KeyTag,
	};
	use reifydb_flow::{
		engine::frontier::WatermarkHold,
		operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
		transaction::{
			read::{ReadFrom, read_from},
			substrate::{FlowSubstrate, apply_operator_state},
			watermark::SourceWatermarks,
		},
	};
	use reifydb_rql::flow::{
		flow::FlowDag,
		operator::{FlowEdge, FlowNode, OperatorDef},
	};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_store_cdc::storage::CdcStorage;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::{
		commit::{CommitBegin, CommitHandle},
		transaction::Transaction,
	};
	use reifydb_value::{factory::time::at_millis, value::identity::IdentityId};

	use super::*;
	use crate::{
		catalog::FlowCatalog,
		commit::{
			committer::{Committer, CommitterActor, CommitterHandle, CommitterMessage},
			quiescence::FlowMaterialization,
		},
		discovery::routing,
		progress::tracker::FlowPositionTracker,
	};

	fn spawn_committer(engine: &StandardEngine) -> CommitterHandle {
		let committer = Committer::new(
			FlowPositionTracker::new(),
			FlowMaterialization::new(CdcConsumerWatermark::new(), FlowPositionTracker::new()),
			engine.operator_state(),
		);
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		engine.spawner()
			.spawn_flow("slice-test-committer", CommitterActor::new(committer, CommitHandle::new(begin)))
	}

	fn commit(committer: &CommitterHandle, slice: FlowSlice) -> CommitVersion {
		let (sender, receiver) = mpsc::channel();
		let sent = committer
			.actor_ref()
			.send(CommitterMessage::Slice {
				slice,
				reply: Box::new(move |result| {
					let _ = sender.send(result);
				}),
			})
			.is_ok();
		assert!(sent, "the committer must accept the slice");
		receiver.recv_timeout(StdDuration::from_secs(10)).expect("slice reply timed out").expect("commit slice")
	}

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
			items.last().expect("has_more implies items").version.commit
		} else {
			safe
		};
		Some(computer
			.compute_pulled(flow_engine, &items, cursor, advance_to, more, config)
			.expect("compute_pulled"))
	}

	fn build_flow_engine(engine: &StandardEngine) -> FlowEngineInner {
		FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
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
	) -> DeferredTransaction {
		let (_current, lease) = engine.acquire_current_snapshot_lease().unwrap();
		let mut query = engine.multi().begin_query_at_version(&lease).unwrap();
		let state_query = engine.multi().begin_query_at_version(&lease).unwrap();
		query.read_as_of_version_inclusive(version);

		DeferredTransaction::new(DeferredParams {
			version,
			pending: Pending::new(),
			query: Some(query),
			state_query: Some(state_query),
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
			},
		));
		builder.add_edge(FlowEdge::new(1, OperatorId(1), OperatorId(3))).unwrap();
		flow_engine.register_flow_dag(builder.build());
		flow_engine.add_sink(flow, OperatorId(3), ObjectId::View(ViewId(3)));

		let mut seed = seeding_txn(&engine, &flow_engine, version);
		SourceWatermarks::advance(OperatorId(1), &mut seed, at_millis(30_000)).unwrap();
		// "Restored state" means the watermark row is in the store; an uncommitted advance is not restored
		// state.
		let seeded = seed.take_pending();
		apply_operator_state(&engine.operator_state(), &seeded);

		let computer = SliceComputer::new(engine.clone());
		let (held, _) = computer.resolved_holds(&mut flow_engine, flow, version).unwrap();

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
			let view_kind = |vid| flow_catalog.find_view(vid).map(|v| v.kind());
			routing::flow_source_objects(&graph, flow_id, &registered, &view_kind)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = spawn_committer(&engine);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut durable = CommitVersion(0);
		let mut committed_any = false;

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
					has_readers: true,
				},
				&config,
			) {
				Some(SliceStep::Commit {
					slice,
					advance_to,
					..
				}) => {
					commit(&committer, slice);
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
	fn pinned_slice_reads_prior_commit_from_the_store() {
		// Output rows commit above the version pinning the next slice, so a store read that misses them loses
		// the flow's own rows.
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
			let view_kind = |vid| flow_catalog.find_view(vid).map(|v| v.kind());
			routing::flow_source_objects(&graph, flow_id, &registered, &view_kind)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = spawn_committer(&engine);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);

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
					has_readers: true,
				},
				&config,
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
					let row_keys: Vec<_> = slice
						.combined
						.iter_sorted()
						.filter(|(k, w)| {
							matches!(KeyTag::of(k), Some(KeyTag::Row))
								&& matches!(w, PendingWrite::Set(_))
						})
						.map(|(k, _)| k.clone())
						.collect();
					assert!(!row_keys.is_empty(), "the slice must have produced view rows");
					let commit_version = commit(&committer, slice);
					assert!(
						commit_version.0 > advance_to.0 + 1,
						"the slice output must commit beyond the read window pinned at chunk_end"
					);

					let mut pinned = DeferredTransaction::new(DeferredParams {
						version: advance_to,
						pending: Pending::new(),
						query: Some(engine.multi().begin_query().unwrap()),
						state_query: Some(engine.multi().begin_query().unwrap()),
						catalog: engine.catalog(),
						interceptors: engine.create_interceptors(),
						clock: engine.clock().clone(),
						substrate: flow_engine.substrate().clone(),
					});
					for key in &row_keys {
						assert!(
							pinned.get(key).unwrap().is_some(),
							"a txn pinned below the flow's commit version must read the flow's own rows from the store"
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
		// A Query-routed write is read back through the query pinned at the cursor, which never sees the flow's
		// own later commit.
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
			let view_kind = |vid| flow_catalog.find_view(vid).map(|v| v.kind());
			routing::flow_source_objects(&graph, flow_id, &registered, &view_kind)
		};

		let computer = SliceComputer::new(engine.clone());
		let committer = spawn_committer(&engine);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		let mut durable = CommitVersion(0);
		let mut committed_kinds: HashSet<Option<KeyTag>> = HashSet::new();
		let mut stale_reads: HashSet<Option<KeyTag>> = HashSet::new();

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
					has_readers: true,
				},
				&config,
			) {
				Some(SliceStep::Commit {
					slice,
					advance_to,
					..
				}) => {
					let mut live_keys = Vec::new();
					for (key, write) in slice.combined.iter_sorted() {
						committed_kinds.insert(KeyTag::of(key));
						if read_from(key) == ReadFrom::Query {
							stale_reads.insert(KeyTag::of(key));
						}
						if matches!(write, PendingWrite::Set(_)) {
							live_keys.push(key.clone());
						}
					}
					commit(&committer, slice);
					// Asserted for operator state as well as rows, since both must resolve from the
					// store alone.
					let mut pinned = DeferredTransaction::new(DeferredParams {
						version: advance_to,
						pending: Pending::new(),
						query: Some(engine.multi().begin_query().unwrap()),
						state_query: Some(engine.multi().begin_query().unwrap()),
						catalog: engine.catalog(),
						interceptors: engine.create_interceptors(),
						clock: engine.clock().clone(),
						substrate: flow_engine.substrate().clone(),
					});
					for key in &live_keys {
						assert!(
							pinned.get(key).unwrap().is_some(),
							"{:?} must resolve from the store with no pending writes",
							KeyTag::of(key)
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
			committed_kinds.contains(&Some(KeyTag::OperatorState)),
			"expected the aggregate to commit operator state, saw only {committed_kinds:?}"
		);
		assert!(
			committed_kinds.contains(&Some(KeyTag::Row)),
			"expected the aggregate to commit view rows, saw only {committed_kinds:?}"
		);

		assert!(
			stale_reads.is_empty(),
			"a flow committed keys that it would read back through the version-pinned query: {stale_reads:?}. \
			 Those reads cannot see the flow's own commit, so the flow reads stale state for them"
		);
	}
	#[test]
	fn a_step_whose_operators_emit_nothing_skips_the_commit_until_the_checkpoint_is_due() {
		// A no-output step must skip its commit, yet past the checkpoint lag it must persist one or CDC stalls.
		let te = TestEngine::builder().with_cdc().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin(
			"CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t FILTER { val > 100 } MAP { id, val } }",
		);
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}]");
		let inserted = te.inner().done_until();

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
			let view_kind = |vid| flow_catalog.find_view(vid).map(|v| v.kind());
			routing::flow_source_objects(&graph, flow_id, &registered, &view_kind)
		};

		let computer = SliceComputer::new(engine.clone());
		let far = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let mut cursor = CommitVersion(0);
		for _ in 0..400 {
			if cursor >= inserted {
				break;
			}
			match pull_step(
				&engine,
				&computer,
				&mut flow_engine,
				SliceCursor {
					flow_id,
					source_objects: &source_objects,
					completeness_objects: None,
					cursor,
					durable_cursor: CommitVersion(0),
					has_readers: true,
				},
				&far,
			) {
				Some(SliceStep::Commit {
					slice,
					..
				}) => panic!(
					"a step with no writes and no view changes committed {} writes and {} view changes \
					 while the checkpoint was not due",
					slice.combined.len(),
					slice.view_changes.len()
				),
				Some(SliceStep::Skip {
					advance_to,
					..
				}) => cursor = advance_to,
				None => sleep(StdDuration::from_millis(5)),
			}
		}
		assert!(cursor >= inserted, "the flow never stepped past the filtered insert at {inserted:?}");

		let due = SliceConfig {
			checkpoint_lag: 0,
		};
		match pull_step(
			&engine,
			&computer,
			&mut flow_engine,
			SliceCursor {
				flow_id,
				source_objects: &source_objects,
				completeness_objects: None,
				cursor: CommitVersion(0),
				durable_cursor: CommitVersion(0),
				has_readers: true,
			},
			&due,
		) {
			Some(SliceStep::Commit {
				slice,
				advance_to,
				..
			}) => {
				assert_eq!(slice.checkpoints, vec![(flow_id, advance_to)]);
				assert!(slice.combined.is_empty(), "the filtered step must carry only its checkpoint");
			}
			Some(SliceStep::Skip {
				advance_to,
				..
			}) => panic!(
				"a step past the checkpoint lag skipped to {advance_to:?} without persisting its checkpoint"
			),
			None => panic!("the insert was already visible, so the replay from zero must produce a step"),
		}
		assert_eq!(view_row_count(&te, "FROM app::v"), 0);
	}

	fn one_view_flow(te: &TestEngine) -> (FlowId, FlowEngineInner, BTreeSet<ObjectId>) {
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
			let view_kind = |vid| flow_catalog.find_view(vid).map(|v| v.kind());
			routing::flow_source_objects(&graph, flow_id, &registered, &view_kind)
		};
		(flow_id, flow_engine, source_objects)
	}

	fn three_separate_inserts(te: &TestEngine) -> CommitVersion {
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		te.command("INSERT app::t [{id: 1, val: 10}]");
		te.command("INSERT app::t [{id: 2, val: 20}]");
		te.command("INSERT app::t [{id: 3, val: 30}]");
		let inserted = te.inner().current_version().expect("current version");
		for _ in 0..400 {
			let safe = te.inner().cdc_producer_watermark().min(te.inner().done_until());
			if safe >= inserted {
				return inserted;
			}
			sleep(StdDuration::from_millis(5));
		}
		panic!("the cdc producer never caught up to {inserted:?}");
	}

	#[test]
	fn a_flow_with_readers_takes_one_source_version_per_step() {
		// A reader merges its producers by source version, so a step that spans two of them would let one
		// producer run ahead of the other inside a single commit and reorder what the reader sees.
		let te = TestEngine::builder().with_cdc().build();
		three_separate_inserts(&te);
		let engine = te.inner().clone();
		let (flow_id, mut flow_engine, source_objects) = one_view_flow(&te);

		let computer = SliceComputer::new(engine.clone());
		let committer = spawn_committer(&engine);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let step = pull_step(
			&engine,
			&computer,
			&mut flow_engine,
			SliceCursor {
				flow_id,
				source_objects: &source_objects,
				completeness_objects: None,
				cursor: CommitVersion(0),
				durable_cursor: CommitVersion(0),
				has_readers: true,
			},
			&config,
		)
		.expect("the inserts are visible, so the flow must step");

		match step {
			SliceStep::Commit {
				slice,
				more,
				..
			} => {
				assert!(more, "a step cut at a source boundary must report more work");
				commit(&committer, slice);
				assert_eq!(
					view_row_count(&te, "FROM app::v"),
					1,
					"the step must carry only the first of the three source versions"
				);
			}
			SliceStep::Skip {
				advance_to,
				..
			} => panic!("the first insert must produce view rows, not a skip to {advance_to:?}"),
		}
	}

	#[test]
	fn a_flow_with_no_readers_folds_every_pending_source_version_into_one_step() {
		// Nothing reads this flow's output by source version, so a flow that fell behind must catch up in
		// one step; cutting per source version caps it at one commit per source version forever.
		let te = TestEngine::builder().with_cdc().build();
		let inserted = three_separate_inserts(&te);
		let engine = te.inner().clone();
		let (flow_id, mut flow_engine, source_objects) = one_view_flow(&te);

		let computer = SliceComputer::new(engine.clone());
		let committer = spawn_committer(&engine);
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let step = pull_step(
			&engine,
			&computer,
			&mut flow_engine,
			SliceCursor {
				flow_id,
				source_objects: &source_objects,
				completeness_objects: None,
				cursor: CommitVersion(0),
				durable_cursor: CommitVersion(0),
				has_readers: false,
			},
			&config,
		)
		.expect("the inserts are visible, so the flow must step");

		match step {
			SliceStep::Commit {
				slice,
				advance_to,
				more,
				..
			} => {
				assert!(!more, "the fold consumed the whole batch, so nothing must be left over");
				assert!(
					advance_to >= inserted,
					"the folded step must advance past the last insert, got {advance_to:?} for {inserted:?}"
				);
				commit(&committer, slice);
				assert_eq!(
					view_row_count(&te, "FROM app::v"),
					3,
					"all three source versions must materialize in the single folded step"
				);
			}
			SliceStep::Skip {
				advance_to,
				..
			} => panic!("the inserts must produce view rows, not a skip to {advance_to:?}"),
		}
	}

	#[test]
	fn a_flow_with_no_readers_that_merges_two_sources_still_takes_one_source_version_per_step() {
		// A flow that merges two inputs pairs them by source version inside its own join, so folding would let
		// one input run ahead of the other within a step even though nothing downstream reads this flow.
		let te = TestEngine::builder().with_cdc().build();
		three_separate_inserts(&te);
		let engine = te.inner().clone();
		let (flow_id, mut flow_engine, source_objects) = one_view_flow(&te);
		let mut merging = source_objects.clone();
		merging.insert(ObjectId::Table(TableId(u64::MAX)));
		assert_eq!(
			merging.len(),
			2,
			"the second source must widen the set, otherwise this repeats the fold test"
		);

		let computer = SliceComputer::new(engine.clone());
		let config = SliceConfig {
			checkpoint_lag: 10_000,
		};

		let step = pull_step(
			&engine,
			&computer,
			&mut flow_engine,
			SliceCursor {
				flow_id,
				source_objects: &merging,
				completeness_objects: None,
				cursor: CommitVersion(0),
				durable_cursor: CommitVersion(0),
				has_readers: false,
			},
			&config,
		)
		.expect("the inserts are visible, so the flow must step");

		match step {
			SliceStep::Commit {
				more,
				..
			} => assert!(more, "a merging flow must stop at the first source boundary"),
			SliceStep::Skip {
				advance_to,
				..
			} => panic!("the first insert must produce view rows, not a skip to {advance_to:?}"),
		}
	}
}
