// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	iter::once,
	num::NonZeroU64,
	sync::Arc,
};

use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::pending::Pending,
	common::{ChangeVersion, CommitVersion, SourceVersion},
	flow::{
		dag::FlowDag,
		operator::{FlowNode, OperatorDef},
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			flow::{FlowId, OperatorId},
			id::ViewId,
			object::ObjectId,
		},
		change::{Change, Diff},
		operator::FlowCheckpoint,
	},
	internal_err,
	key::{
		ringbuffer::{
			PartitionedRingBufferOrderKey, RingBufferFreeKey, RingBufferMetadataKey, RingBufferOrderKey,
		},
		row::{PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, SortedViewRowKey},
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	},
};
use reifydb_engine::{backfill::run, engine::StandardEngine, lift::entry_changed_objects};
use reifydb_flow::txn::FlowBatch;
use reifydb_flow_async::{
	engine::{FlowEngineInner, frontier::WatermarkHolds},
	operator::InputOrder,
	transaction::{DeferredParams, FlowTransaction, deferred::DeferredTransaction},
};
use reifydb_runtime::context::transaction::TransactionContext;
use reifydb_transaction::{
	multi::{RangeScope, lease::VersionLeaseGuard},
	transaction::Transaction,
};
use reifydb_value::{
	Result,
	value::{
		identity::IdentityId,
		system_columns::{SystemColumn, require_updated_at, system_column},
	},
};
use reifydb_wal::lsn::Lsn;
use rustc_hash::FxHashSet;
use smallvec::smallvec;

use crate::{
	commit::{committer::FlowSlice, merge::StreamRead, slice::process_unit},
	progress::tracker::FlowUpstreams,
};

pub(crate) struct Snapshot {
	pub(crate) version: CommitVersion,
	pub(crate) lsn: Lsn,
	pub(crate) lease: VersionLeaseGuard,
	pub(crate) cuts: HashMap<FlowId, UpstreamCut>,
}

#[derive(Clone, Copy)]
pub(crate) struct UpstreamCut {
	pub(crate) read_to: CommitVersion,
	pub(crate) at: Option<CommitVersion>,
}

pub(crate) fn backfill_reads(
	flow: &FlowDag,
	flow_engine: &FlowEngineInner,
	snapshot: &Snapshot,
	upstreams: &FlowUpstreams,
) -> Vec<(CommitVersion, Vec<ObjectId>)> {
	let mut reads: Vec<(CommitVersion, Vec<ObjectId>)> = Vec::new();
	for source in feed_order(flow, flow_engine) {
		let at = read_at(source, snapshot, upstreams);
		match reads.last_mut() {
			Some((last, sources)) if *last == at => sources.push(source),
			_ => reads.push((at, vec![source])),
		}
	}
	reads
}

fn read_at(source: ObjectId, snapshot: &Snapshot, upstreams: &FlowUpstreams) -> CommitVersion {
	if !matches!(source, ObjectId::View(_)) {
		return snapshot.version;
	}
	upstreams
		.iter()
		.find(|(_, produced)| produced.contains(&source))
		.and_then(|(producer, _)| snapshot.cuts.get(producer))
		.and_then(|cut| cut.at)
		.unwrap_or(snapshot.version)
}

fn lookup_right_inputs(flow: &FlowDag) -> HashSet<OperatorId> {
	flow.get_operator_ids()
		.filter_map(|id| flow.get_operator(&id))
		.filter(|node| matches!(node.ty, OperatorDef::Lookup { .. }))
		.filter_map(|node| node.inputs.get(1).copied())
		.collect()
}

fn feed_order(flow: &FlowDag, flow_engine: &FlowEngineInner) -> Vec<ObjectId> {
	let mut upstream: HashMap<OperatorId, BTreeSet<ObjectId>> = HashMap::new();
	let mut after: BTreeMap<ObjectId, BTreeSet<ObjectId>> = BTreeMap::new();
	let mut sources: BTreeSet<ObjectId> = BTreeSet::new();
	let lookup_rights = lookup_right_inputs(flow);
	for id in flow.topological_order() {
		let Some(node) = flow.get_operator(id) else {
			continue;
		};
		let mut reached: BTreeSet<ObjectId> = match lookup_rights.contains(id) {
			true => BTreeSet::new(),
			false => source_object(node).into_iter().collect(),
		};
		sources.extend(reached.iter().copied());
		for input in &node.inputs {
			if let Some(inputs) = upstream.get(input) {
				reached.extend(inputs.iter().copied());
			}
		}
		for (earlier, later) in ranked_pairs(flow, flow_engine, node) {
			for first in upstream.get(&earlier).into_iter().flatten() {
				for second in upstream.get(&later).into_iter().flatten() {
					if first != second {
						after.entry(*first).or_default().insert(*second);
					}
				}
			}
		}
		upstream.insert(*id, reached);
	}

	let mut blockers: BTreeMap<ObjectId, usize> = sources.iter().map(|source| (*source, 0)).collect();
	for second in after.values().flatten() {
		if let Some(count) = blockers.get_mut(second) {
			*count += 1;
		}
	}
	let mut remaining: BTreeSet<(bool, ObjectId)> =
		sources.iter().map(|source| (!matches!(source, ObjectId::View(_)), *source)).collect();
	let mut order = Vec::with_capacity(remaining.len());
	while let Some(next) = remaining
		.iter()
		.find(|(_, source)| blockers.get(source).is_some_and(|count| *count == 0))
		.or_else(|| remaining.first())
		.copied()
	{
		remaining.remove(&next);
		order.push(next.1);
		for second in after.get(&next.1).into_iter().flatten() {
			if let Some(count) = blockers.get_mut(second) {
				*count -= 1;
			}
		}
	}
	order
}

fn ranked_pairs(flow: &FlowDag, flow_engine: &FlowEngineInner, node: &FlowNode) -> Vec<(OperatorId, OperatorId)> {
	let arity = node.inputs.len();
	let order = flow_engine
		.operator(flow.id, node.id)
		.map(|operator| operator.input_order())
		.unwrap_or(InputOrder::Declared);
	if order == InputOrder::Declared || arity < 2 {
		return Vec::new();
	}
	let mut ranked: Vec<(usize, OperatorId)> =
		node.inputs.iter().enumerate().map(|(position, input)| (order.rank(position, arity), *input)).collect();
	ranked.sort();
	let mut pairs = Vec::new();
	for (index, (_, earlier)) in ranked.iter().enumerate() {
		for (_, later) in &ranked[index + 1..] {
			pairs.push((*earlier, *later));
		}
	}
	pairs
}

fn source_object(node: &FlowNode) -> Option<ObjectId> {
	match &node.ty {
		OperatorDef::SourceView {
			view,
		} => Some(ObjectId::view(*view)),
		ty => ty.source_object_id(),
	}
}

pub(crate) fn upstream_write_after(
	read: &StreamRead,
	from: CommitVersion,
	views: &FxHashSet<ObjectId>,
	version: CommitVersion,
) -> Option<CommitVersion> {
	read.items
		.iter()
		.filter(|entry| entry.version.commit > from && entry.version.source.0 > version.0)
		.find(|entry| entry_changed_objects(entry).iter().any(|object| views.contains(object)))
		.map(|entry| CommitVersion(entry.version.commit.0 - 1))
}
pub(crate) fn compute_unit_backfill(
	engine: &StandardEngine,
	flow_engine: &mut FlowEngineInner,
	flow: &FlowDag,
	members: &[(FlowDag, ViewId)],
	snapshot: &Snapshot,
	reads: &[(CommitVersion, Vec<ObjectId>)],
) -> Result<(FlowSlice, WatermarkHolds)> {
	let version = snapshot.version;
	let fed: Vec<(FlowId, ViewId)> = members.iter().map(|(member, view)| (member.id, *view)).collect();
	let batch_size = query_batch_size(engine)?;
	let services = engine.services();
	let mut txn = DeferredTransaction::new(DeferredParams {
		ctx: Arc::new(TransactionContext::system(engine.runtime_context().clone())),
		version,
		pending: Pending::new(),
		query: Some(engine
			.multi()
			.begin_query_at_version(engine.multi().generate_transaction_id(), &snapshot.lease)?),
		state_query: Some(engine
			.multi()
			.begin_query_at_version(engine.multi().generate_transaction_id(), &snapshot.lease)?),
		catalog: engine.catalog(),
		interceptors: engine.create_interceptors(),
		clock: engine.clock().clone(),
		substrate: flow_engine.substrate().clone(),
		lookup: None,
	});

	clear_views(engine, &mut txn, flow, &snapshot.lease, batch_size)?;
	for (member, _) in members {
		clear_views(engine, &mut txn, member, &snapshot.lease, batch_size)?;
	}

	for (at, sources) in reads {
		let lease = if *at == version {
			snapshot.lease.clone()
		} else {
			engine.acquire_version_lease(*at)?
		};
		let mut query = engine.begin_query_at_version(&lease, IdentityId::system())?;
		for source in sources {
			run(
				&services,
				Transaction::Query(&mut query),
				&BTreeSet::from([*source]),
				batch_size,
				|_, mut change| {
					change.version = ChangeVersion::from(version);
					for piece in time_runs(change)? {
						process_unit(flow_engine, &mut txn, flow.id, &fed, vec![piece])?;
					}
					Ok(())
				},
			)?;
		}
	}

	let mut holds = flow_engine.holds(&mut txn, flow.id)?;
	for (member, _) in &fed {
		holds.extend(flow_engine.holds(&mut txn, *member)?);
	}
	let emitted = txn.emitted;

	let slice = FlowSlice {
		combined: txn.take_pending(),
		checkpoints: once(flow.id)
			.chain(fed.iter().map(|(member, _)| *member))
			.map(|id| {
				(
					id,
					FlowCheckpoint {
						version,
						lsn: snapshot.lsn,
					},
				)
			})
			.collect(),
		checkpoint_deletes: Vec::new(),
		emitted,
		source: Some(SourceVersion(version.0)),
		written_views: Vec::new(),
		input_markers: None,
		layers: Vec::new(),
	};
	Ok((slice, holds))
}

fn time_runs(change: Change) -> Result<Vec<Change>> {
	let mut pieces = Vec::new();
	for diff in change.diffs {
		let Diff::Insert {
			post,
			origin,
		} = diff
		else {
			return internal_err!(
				"a snapshot change from {:?} holds a diff that is not an insert",
				change.origin
			);
		};
		for rows in equal_time_rows(&post)? {
			let Some(changed_at) = require_updated_at(&rows)?.iter().copied().max() else {
				return internal_err!(
					"a snapshot run from {:?} has no updated_at stamps",
					change.origin
				);
			};
			pieces.push(Change {
				origin: change.origin.clone(),
				diffs: smallvec![Diff::Insert {
					post: rows,
					origin: origin.clone(),
				}],
				version: change.version,
				changed_at,
			});
		}
	}
	Ok(pieces)
}

fn equal_time_rows(post: &RecordBatch) -> Result<Vec<RecordBatch>> {
	let Some(column) = system_column(post, SystemColumn::Time) else {
		return Ok(vec![post.clone()]);
	};
	let Some(times) = column.as_any().downcast_ref::<TimestampNanosecondArray>() else {
		return internal_err!(
			"system column {} holds arrow type {}",
			SystemColumn::Time.name(),
			column.data_type()
		);
	};
	let at = |row: usize| times.is_valid(row).then(|| times.value(row));
	let mut runs = Vec::new();
	let mut start = 0;
	for row in 1..=post.num_rows() {
		if row == post.num_rows() || at(row) != at(start) {
			runs.push(post.slice(start, row - start));
			start = row;
		}
	}
	Ok(runs)
}

fn clear_views(
	engine: &StandardEngine,
	txn: &mut DeferredTransaction,
	flow: &FlowDag,
	lease: &VersionLeaseGuard,
	batch_size: NonZeroU64,
) -> Result<()> {
	let catalog = engine.catalog();
	let mut query = engine.begin_query_at_version(lease, IdentityId::system())?;
	let mut keys: Vec<EncodedKey> = Vec::new();
	for view in flow.sink_views() {
		let storage = catalog.get_view(&mut Transaction::Query(&mut query), view)?.storage_id();
		let ranges = [
			RowKey::full_scan(storage),
			PartitionedRowKey::full_scan(storage),
			SortedViewRowKey::storage_scan(storage),
			PartitionedSortedViewRowKey::storage_scan(storage),
			SeriesRowKeyRange::full_scan(storage, None),
			PartitionedSeriesRowKeyRange::full_scan(storage),
			RingBufferMetadataKey::full_scan_for_storage(storage),
			RingBufferOrderKey::full_scan_for_storage(storage),
			PartitionedRingBufferOrderKey::full_scan_for_storage(storage),
			RingBufferFreeKey::full_scan_for_storage(storage),
		];
		for range in ranges {
			for row in txn.range(range, RangeScope::All, batch_size.get() as usize) {
				keys.push(row?.key.encode());
			}
		}
	}
	txn.remove(keys)?;
	Ok(())
}

fn query_batch_size(engine: &StandardEngine) -> Result<NonZeroU64> {
	match NonZeroU64::new(u64::from(engine.catalog().get_config_uint2(ConfigKey::QueryRowBatchSize))) {
		Some(batch_size) => Ok(batch_size),
		None => internal_err!("QUERY_ROW_BATCH_SIZE is 0, which its config validation rejects"),
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, mpsc};

	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		actors::pending::PendingWrite,
		commit::watermark::ReaderWatermark,
		common::{JoinType, TimeDomain},
		flow::operator::LookupObject,
		interface::catalog::{flow::FlowEdge, id::TableId},
		operator_with::LookupWith,
	};
	use reifydb_flow_async::{
		operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
		transaction::{FlowTransaction, substrate::FlowSubstrate},
	};
	use reifydb_runtime::context::{RuntimeContext, spawner::Spawner, transaction::TransactionContext};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::commit::{CommitBegin, CommitHandle};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{duration::Duration, partition::Partition},
	};

	use super::*;
	use crate::{
		commit::{
			committer::{Committer, CommitterActor, CommitterMessage},
			quiescence::FlowMaterialization,
		},
		progress::tracker::FlowPositionTracker,
	};

	fn system_ctx() -> Arc<TransactionContext> {
		Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
	}

	fn backfill_and_commit(te: &TestEngine) -> (CommitVersion, FlowMaterialization) {
		let engine = te.inner().clone();
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flow_id = engine
			.catalog()
			.list_flows_all(&mut Transaction::Query(&mut query))
			.expect("list flows")
			.first()
			.expect("one flow")
			.id;
		drop(query);

		let mut flow_engine = FlowEngineInner::new(
			engine.catalog(),
			engine.executor().routines.clone(),
			RuntimeContext::builder(Spawner::inline()).clock(engine.clock().clone()).build(),
			Arc::new(EmptyOperatorProvider),
			FlowSubstrate::with_dictionary(engine.dictionary_allocators(), engine.operator_state()),
			OperatorSampleRegistry::new(),
		);
		let mut txn = engine.begin_command(IdentityId::system()).expect("command");
		let flow =
			engine.catalog().get_flow_dag(&mut Transaction::Command(&mut txn), flow_id).expect("load flow");
		flow_engine.register(&mut txn, flow.clone()).expect("register");
		txn.rollback().expect("rollback registration probe");

		let (version, lease) = engine.acquire_current_snapshot_lease().expect("snapshot lease");
		let snapshot = Snapshot {
			version,
			lsn: Lsn::new(0),
			lease,
			cuts: HashMap::new(),
		};
		let reads = backfill_reads(&flow, &flow_engine, &snapshot, &FlowUpstreams::default());
		let (slice, _) = compute_unit_backfill(&engine, &mut flow_engine, &flow, &[], &snapshot, &reads)
			.expect("compute backfill");
		drop(snapshot);

		let materialization = FlowMaterialization::new(ReaderWatermark::new(), FlowPositionTracker::new());
		let committer =
			Committer::new(FlowPositionTracker::new(), materialization.clone(), engine.operator_state());
		let begin_engine = engine.clone();
		let begin: CommitBegin = Arc::new(move || begin_engine.begin_command(IdentityId::system()));
		let handle = engine.spawner().spawn_flow(
			"backfill-test-committer",
			CommitterActor::new(committer, CommitHandle::new(begin)),
		);
		let (sender, receiver) = mpsc::channel();
		assert!(
			handle.actor_ref()
				.send(CommitterMessage::Slice {
					slice,
					reply: Box::new(move |result| {
						let _ = sender.send(result);
					}),
				})
				.is_ok(),
			"the committer must accept the backfill slice"
		);
		let committed = receiver
			.recv_timeout(Duration::from_seconds_const(10).to_std())
			.expect("backfill reply timed out")
			.expect("commit backfill");
		(committed, materialization)
	}

	#[test]
	fn a_backfill_reset_removes_every_free_page_of_the_view() {
		// A page left behind would hand slots of the old contents to the rebuilt view.
		let te = TestEngine::builder().with_commit_queue().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");
		let engine = te.inner().clone();
		let mut query = engine.begin_query(IdentityId::system()).expect("query");
		let flow = engine
			.catalog()
			.list_flow_dags_asc(&mut Transaction::Query(&mut query))
			.expect("list flows")
			.into_iter()
			.next()
			.expect("one flow");
		let view = flow.sink_views().next().expect("one sink view");
		let storage = engine
			.catalog()
			.get_view(&mut Transaction::Query(&mut query), view)
			.expect("view")
			.storage_id();
		drop(query);
		let pages = [
			RingBufferFreeKey::of(storage, None, 0),
			RingBufferFreeKey::of(storage, Some(Partition(1)), 3),
		];
		let mut seed = engine.begin_command(IdentityId::system()).expect("command");
		for page in &pages {
			seed.set(vec![(page.clone().into(), EncodedBytes(CowVec::new(vec![0; 8])))]).expect("seed");
		}
		seed.commit().expect("commit");

		let (version, lease) = engine.acquire_current_snapshot_lease().expect("snapshot lease");
		let mut txn = DeferredTransaction::new(DeferredParams {
			ctx: system_ctx(),
			version,
			pending: Pending::new(),
			query: Some(engine
				.multi()
				.begin_query_at_version(engine.multi().generate_transaction_id(), &lease)
				.expect("query")),
			state_query: Some(engine
				.multi()
				.begin_query_at_version(engine.multi().generate_transaction_id(), &lease)
				.expect("query")),
			catalog: engine.catalog(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			substrate: FlowSubstrate::with_dictionary(
				engine.dictionary_allocators(),
				engine.operator_state(),
			),
			lookup: None,
		});
		clear_views(&engine, &mut txn, &flow, &lease, query_batch_size(&engine).expect("batch size"))
			.expect("clear");
		for page in &pages {
			assert!(
				matches!(txn.pending().write_at(&page.encode()), Some(PendingWrite::Remove { .. })),
				"{page:?} must be removed"
			);
		}
	}

	#[test]
	fn a_backfill_over_existing_rows_commits_them_and_advances_the_output_frontier() {
		// A new view's readers wait on the output frontier; a backfill that stores rows without moving it
		// leaves them waiting for rows that are already there.
		let te = TestEngine::builder().with_commit_queue().build();
		te.admin("CREATE NAMESPACE app");
		te.admin("CREATE TABLE app::t { id: int4, val: int4 }");
		te.command("INSERT app::t [{id: 1, val: 10}, {id: 2, val: 20}, {id: 3, val: 30}]");
		te.admin("CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }");

		let (version, materialization) = backfill_and_commit(&te);

		assert_eq!(materialization.output_frontier(), version);
		assert_eq!(te.query("FROM app::v").first().map(|f| f.row_count()).unwrap_or(0), 3);
	}

	#[test]
	fn a_lookup_right_input_is_not_fed_at_backfill_and_a_left_input_is() {
		// feeding the right table would scan all of it to probe an index that is still empty.
		const LEFT_SOURCE: OperatorId = OperatorId(1);
		const RIGHT_SOURCE: OperatorId = OperatorId(2);
		const LOOKUP: OperatorId = OperatorId(3);
		let source = || OperatorDef::SourceTable {
			table: TableId(1),
			time_domain: TimeDomain::None,
		};
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(LEFT_SOURCE, source()));
		builder.add_node(FlowNode::new(RIGHT_SOURCE, source()));
		builder.add_node(FlowNode::new(
			LOOKUP,
			OperatorDef::Lookup {
				join_type: JoinType::Inner,
				right: LookupObject::Table(TableId(1)),
				left: Vec::new(),
				alias: None,
				with: LookupWith::default(),
			},
		));
		builder.add_edge(FlowEdge::new(1, builder.id(), LEFT_SOURCE, LOOKUP)).unwrap();
		builder.add_edge(FlowEdge::new(2, builder.id(), RIGHT_SOURCE, LOOKUP)).unwrap();

		let rights = lookup_right_inputs(&builder.build());

		assert!(rights.contains(&RIGHT_SOURCE));
		assert!(!rights.contains(&LEFT_SOURCE));
	}
}
