// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	num::NonZeroU64,
};

use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use reifydb_cdc::lift::changed_objects;
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
			object::ObjectId,
		},
		change::{Change, Diff},
	},
	internal_err,
	key::{
		ringbuffer::RingBufferMetadataKey,
		row::{PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, SortedViewRowKey},
		series::{PartitionedSeriesRowKeyRange, SeriesRowKeyRange},
	},
};
use reifydb_engine::{backfill::run, engine::StandardEngine};
use reifydb_flow_async::{
	engine::{FlowEngineInner, frontier::WatermarkHolds},
	operator::InputOrder,
	transaction::{DeferredParams, FlowTransaction, deferred::DeferredTransaction},
};
use reifydb_transaction::{
	accumulator::ChangeAccumulator,
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
use rustc_hash::FxHashSet;
use smallvec::smallvec;

use crate::{
	commit::{committer::FlowSlice, merge::StreamRead},
	progress::tracker::FlowUpstreams,
};

pub(crate) struct Snapshot {
	pub(crate) version: CommitVersion,
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

fn feed_order(flow: &FlowDag, flow_engine: &FlowEngineInner) -> Vec<ObjectId> {
	let mut upstream: HashMap<OperatorId, BTreeSet<ObjectId>> = HashMap::new();
	let mut after: BTreeMap<ObjectId, BTreeSet<ObjectId>> = BTreeMap::new();
	let mut sources: BTreeSet<ObjectId> = BTreeSet::new();
	for id in flow.topological_order() {
		let Some(node) = flow.get_operator(id) else {
			continue;
		};
		let mut reached: BTreeSet<ObjectId> = source_object(node).into_iter().collect();
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
		.filter(|cdc| cdc.version.commit > from && cdc.version.source.0 > version.0)
		.find(|cdc| changed_objects(cdc).iter().any(|object| views.contains(object)))
		.map(|cdc| CommitVersion(cdc.version.commit.0 - 1))
}

pub(crate) fn compute_backfill(
	engine: &StandardEngine,
	flow_engine: &mut FlowEngineInner,
	flow: &FlowDag,
	snapshot: &Snapshot,
	reads: &[(CommitVersion, Vec<ObjectId>)],
) -> Result<(FlowSlice, WatermarkHolds)> {
	let version = snapshot.version;
	let batch_size = query_batch_size(engine)?;
	let services = engine.services();
	let mut txn = DeferredTransaction::new(DeferredParams {
		version,
		pending: Pending::new(),
		query: Some(engine.multi().begin_query_at_version(&snapshot.lease)?),
		state_query: Some(engine.multi().begin_query_at_version(&snapshot.lease)?),
		catalog: engine.catalog(),
		interceptors: engine.create_interceptors(),
		clock: engine.clock().clone(),
		substrate: flow_engine.substrate().clone(),
		lookup: None,
	});

	clear_views(engine, &mut txn, flow, &snapshot.lease, batch_size)?;

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
						flow_engine.process_batch(&mut txn, vec![piece], flow.id)?;
					}
					Ok(())
				},
			)?;
		}
	}

	let holds = flow_engine.holds(&mut txn, flow.id)?;
	let mut accumulator = ChangeAccumulator::new();
	for (id, diff) in txn.take_accumulator_entries() {
		accumulator.track(id, diff);
	}
	let view_changes = accumulator.take_changes(version, engine.clock().now())?;

	let slice = FlowSlice {
		combined: txn.take_pending(),
		checkpoints: vec![(flow.id, version)],
		checkpoint_deletes: Vec::new(),
		view_changes,
		control_cursor: None,
		source: Some(SourceVersion(version.0)),
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
		];
		for range in ranges {
			for row in txn.range(range.encode(), RangeScope::All, batch_size.get() as usize) {
				keys.push(row?.key.encode());
			}
		}
	}
	txn.remove_batch(keys)?;
	Ok(())
}

fn query_batch_size(engine: &StandardEngine) -> Result<NonZeroU64> {
	match NonZeroU64::new(u64::from(engine.catalog().get_config_uint2(ConfigKey::QueryRowBatchSize))) {
		Some(batch_size) => Ok(batch_size),
		None => internal_err!("QUERY_ROW_BATCH_SIZE is 0, which its config validation rejects"),
	}
}
