// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::size_of,
	ops::Range,
	sync::{Arc, OnceLock, Weak},
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};
use reifydb_wal::lsn::Lsn;

use crate::{
	commit::queue::{CommitQueue, QueueShared},
	common::{ChangeVersion, CommitVersion, SourceVersion},
	delta::{Delta, RemoveAnnounce, SetPre},
	interface::{catalog::object::ObjectId, change::Change},
	key::{any::TaggedKey, tag::KeyTag},
	wal::CommitLayer,
};

pub fn flow_relevant(delta: &Delta) -> bool {
	!is_commit_excluded(delta.key().kind())
		&& !matches!(
			delta,
			Delta::Remove {
				announce: RemoveAnnounce::Silent,
				..
			}
		)
}

pub fn is_commit_excluded(kind: KeyTag) -> bool {
	matches!(
		kind,
		KeyTag::OperatorState
			| KeyTag::Metric | KeyTag::SystemSequence
			| KeyTag::RowSequence | KeyTag::ColumnSequence
			| KeyTag::SystemVersion
			| KeyTag::FlowVersion | KeyTag::RingBufferMetadata
			| KeyTag::Index | KeyTag::ConfigStorage
			| KeyTag::Token | KeyTag::VersionEpoch
			| KeyTag::QueuePartition
			| KeyTag::QueueItemState
			| KeyTag::QueueDue | KeyTag::QueueKeyActive
	)
}

#[derive(Debug)]
pub struct CommitEntry {
	pub version: ChangeVersion,
	pub lsn: Lsn,
	pub at: DateTime,
	pub deltas: Vec<Delta>,
	pub layers: Vec<(SourceVersion, Range<usize>)>,
	changed_objects: OnceLock<BTreeSet<ObjectId>>,
	lift: OnceLock<CommitLift>,
	queue: OnceLock<Weak<QueueShared>>,
}

#[derive(Debug, Default)]
pub struct CommitLift {
	pub objects: BTreeMap<ObjectId, CommitObjectLift>,
}

#[derive(Debug, Default)]
pub struct CommitObjectLift {
	pub rows: Vec<(usize, RowNumber)>,
	pub changes: OnceLock<Vec<Change>>,
}

impl CommitEntry {
	pub fn build(
		version: ChangeVersion,
		lsn: Lsn,
		at: DateTime,
		deltas: Vec<Delta>,
		layers: Vec<CommitLayer>,
	) -> Self {
		let (deltas, layers) = layered_feed(deltas, layers);
		Self {
			version,
			lsn,
			at,
			deltas,
			layers,
			changed_objects: OnceLock::new(),
			lift: OnceLock::new(),
			queue: OnceLock::new(),
		}
	}

	pub(crate) fn attach(&self, queue: Weak<QueueShared>) {
		let _ = self.queue.set(queue);
	}

	pub fn charge(&self, bytes: u64) {
		let Some(shared) = self.queue.get().and_then(Weak::upgrade) else {
			return;
		};
		CommitQueue::from_shared(shared).charge(self.version.commit, self as *const Self, bytes);
	}

	pub fn changed_objects_with(&self, compute: impl FnOnce(&Self) -> BTreeSet<ObjectId>) -> &BTreeSet<ObjectId> {
		self.changed_objects.get_or_init(|| compute(self))
	}

	pub fn lift_with(&self, build: impl FnOnce(&Self) -> CommitLift) -> &CommitLift {
		self.lift.get_or_init(|| build(self))
	}

	pub fn first_source(&self) -> SourceVersion {
		self.layers.first().map_or(self.version.source, |(source, _)| *source)
	}

	pub fn layers_between(self: &Arc<Self>, after: SourceVersion, through: SourceVersion) -> Option<Arc<Self>> {
		if self.layers.is_empty() {
			return (after < self.version.source && self.version.source <= through)
				.then(|| Arc::clone(self));
		}
		let kept: Vec<&(SourceVersion, Range<usize>)> =
			self.layers.iter().filter(|(source, _)| after < *source && *source <= through).collect();
		let last = kept.last()?.0;
		if kept.len() == self.layers.len() {
			return Some(Arc::clone(self));
		}
		let mut deltas = Vec::new();
		let mut layers = Vec::with_capacity(kept.len());
		for (source, range) in kept {
			let start = deltas.len();
			deltas.extend(self.deltas[range.clone()].iter().cloned());
			layers.push((*source, start..deltas.len()));
		}
		Some(Arc::new(Self {
			version: ChangeVersion {
				commit: self.version.commit,
				source: last,
			},
			lsn: self.lsn,
			at: self.at,
			deltas,
			layers,
			changed_objects: OnceLock::new(),
			lift: OnceLock::new(),
			queue: OnceLock::new(),
		}))
	}

	pub fn source_at(&self, index: usize) -> SourceVersion {
		let layer = self.layers.partition_point(|(_, range)| range.end <= index);
		self.layers.get(layer).map_or(self.version.source, |(source, _)| *source)
	}

	pub fn bytes(&self) -> u64 {
		let deltas: usize = self
			.deltas
			.iter()
			.map(|delta| {
				size_of::<Delta>()
					+ delta.bytes().map_or(0, |bytes| bytes.len())
					+ delta_pre(delta).map_or(0, |pre| pre.len())
			})
			.sum();
		(size_of::<Self>() + deltas) as u64
	}
}

fn layered_feed(deltas: Vec<Delta>, layers: Vec<CommitLayer>) -> (Vec<Delta>, Vec<(SourceVersion, Range<usize>)>) {
	let deltas = deltas.into_iter().filter(flow_relevant);
	if layers.is_empty() {
		return (deltas.collect(), Vec::new());
	}
	let covered: BTreeSet<TaggedKey> =
		layers.iter().flat_map(|layer| layer.deltas.iter().map(|delta| delta.key().clone())).collect();
	let leftover: Vec<Delta> = deltas.filter(|delta| !covered.contains(delta.key())).collect();
	let last = layers.len() - 1;
	let mut feed = Vec::new();
	let mut ranges = Vec::with_capacity(layers.len());
	for (index, layer) in layers.into_iter().enumerate() {
		let start = feed.len();
		feed.extend(layer.deltas.into_iter().filter(flow_relevant));
		if index == last {
			feed.extend(leftover.iter().cloned());
		}
		ranges.push((layer.source, start..feed.len()));
	}
	(feed, ranges)
}

fn delta_pre(delta: &Delta) -> Option<&EncodedBytes> {
	match delta {
		Delta::Set {
			pre: SetPre::Present(pre),
			..
		} => Some(pre),
		Delta::Set {
			..
		} => None,
		Delta::Remove {
			announce,
			..
		} => announce.pre(),
	}
}

#[derive(Debug, Clone)]
pub struct Loaded {
	pub entries: Vec<Arc<CommitEntry>>,
	pub advance_to: CommitVersion,
	pub more: bool,
}

pub trait CommitLog: Send + Sync + 'static {
	fn read(&self, after: (CommitVersion, Lsn), up_to: CommitVersion, budget: ByteSize) -> Result<Loaded>;
}

pub trait CommitFloor: Send + Sync + 'static {
	fn position(&self) -> Option<Lsn>;

	fn advance(&self, lsn: Lsn);
}

#[derive(Clone)]
pub struct FlowWal {
	pub log: Arc<dyn CommitLog>,
	pub floor: Arc<dyn CommitFloor>,
}

#[cfg(test)]
mod exclusion_tests {
	use super::*;

	#[test]
	fn test_all_key_kinds_have_explicit_exclusion_decision() {
		// The exhaustive match forces every new KeyTag to make a commit exclusion decision instead of
		// silently defaulting into the log.

		let test_variant = KeyTag::Row;

		match test_variant {
			KeyTag::Namespace => {}
			KeyTag::Table => {}
			KeyTag::Row => {}
			KeyTag::NamespaceTable => {}
			KeyTag::SystemSequence => {}
			KeyTag::Columns => {}
			KeyTag::Column => {}
			KeyTag::RowSequence => {}
			KeyTag::ColumnProperty => {}
			KeyTag::SystemVersion => {}
			KeyTag::Index => {}
			KeyTag::IndexEntry => {}
			KeyTag::ColumnSequence => {}
			KeyTag::View => {}
			KeyTag::ViewRingBuffer => {}
			KeyTag::ViewQueue => {}
			KeyTag::NamespaceView => {}
			KeyTag::PrimaryKey => {}
			KeyTag::OperatorState => {}
			KeyTag::RingBuffer => {}
			KeyTag::NamespaceRingBuffer => {}
			KeyTag::RingBufferMetadata => {}
			KeyTag::Flow => {}
			KeyTag::NamespaceFlow => {}
			KeyTag::Operator => {}
			KeyTag::OperatorByFlow => {}
			KeyTag::FlowEdge => {}
			KeyTag::FlowEdgeByFlow => {}
			KeyTag::OutputFrontier => {}
			KeyTag::Dictionary => {}
			KeyTag::DictionaryEntry => {}
			KeyTag::DictionaryEntryIndex => {}
			KeyTag::NamespaceDictionary => {}
			KeyTag::Metric => {}
			KeyTag::FlowVersion => {}
			KeyTag::RowShape => {}
			KeyTag::SumType => {}
			KeyTag::NamespaceSumType => {}
			KeyTag::RowShapeField => {}
			KeyTag::Handler => {}
			KeyTag::NamespaceHandler => {}
			KeyTag::VariantHandler => {}
			KeyTag::Series => {}
			KeyTag::NamespaceSeries => {}
			KeyTag::SeriesPartitionMetadata => {}
			KeyTag::RingBufferEviction => {}
			KeyTag::ViewEviction => {}
			KeyTag::TableEviction => {}
			KeyTag::SeriesEviction => {}
			KeyTag::Identity => {}
			KeyTag::Role => {}
			KeyTag::GrantedRole => {}
			KeyTag::Policy => {}
			KeyTag::PolicyOp => {}
			KeyTag::Migration => {}
			KeyTag::Authentication => {}
			KeyTag::AuthenticationMethod => {}
			KeyTag::MigrationEvent => {}
			KeyTag::ConfigStorage => {}
			KeyTag::Token => {}
			KeyTag::Source => {}
			KeyTag::NamespaceSource => {}
			KeyTag::Sink => {}
			KeyTag::NamespaceSink => {}
			KeyTag::WindowedViewGroup => {}
			KeyTag::NamespaceWindowedViewGroup => {}
			KeyTag::RowSettings => {}
			KeyTag::Procedure => {}
			KeyTag::NamespaceProcedure => {}
			KeyTag::ProcedureParam => {}
			KeyTag::Binding => {}
			KeyTag::OperatorRetention => {}
			KeyTag::NamespaceBinding => {}
			KeyTag::ColumnBlock => {}
			KeyTag::SeriesColumnBlock => {}
			KeyTag::TableColumnBlock => {}
			KeyTag::IdentityAttribute => {}
			KeyTag::IdentityAttributeValue => {}
			KeyTag::PartitionedRow => {}
			KeyTag::PartitionedSeriesRow => {}
			KeyTag::Partition => {}
			KeyTag::Queue => {}
			KeyTag::NamespaceQueue => {}
			KeyTag::QueueDeduplication => {}
			KeyTag::QueuePartition => {}
			KeyTag::QueueItemState => {}
			KeyTag::QueueDue => {}
			KeyTag::QueueAttempt => {}
			KeyTag::QueueKeyActive => {}
			KeyTag::VersionEpoch => {}
			KeyTag::SeriesRow => {}
			KeyTag::SortedViewRow => {}
			KeyTag::PartitionedSortedViewRow => {}
			KeyTag::Relationship => {} /* When adding a new variant, add it here.
			                            * The compiler will error if you forget.
			                            * Then add a test and update is_commit_excluded() if
			                            * needed. */
		}
	}

	#[test]
	fn test_sorted_view_rows_reach_the_commit_log() {
		// A sorted view's rows carry their own kind, and a subscriber that never sees them reads a
		// view that silently stops changing.
		assert!(!is_commit_excluded(KeyTag::SortedViewRow));
		assert!(!is_commit_excluded(KeyTag::PartitionedSortedViewRow));
	}

	#[test]
	fn test_exclude_operator_state() {
		assert!(is_commit_excluded(KeyTag::OperatorState));
	}

	#[test]
	fn test_exclude_storage_tracker() {
		assert!(is_commit_excluded(KeyTag::Metric));
	}

	#[test]
	fn test_exclude_system_sequence() {
		assert!(is_commit_excluded(KeyTag::SystemSequence));
	}

	#[test]
	fn test_exclude_row_sequence() {
		assert!(is_commit_excluded(KeyTag::RowSequence));
	}

	#[test]
	fn test_exclude_column_sequence() {
		assert!(is_commit_excluded(KeyTag::ColumnSequence));
	}

	#[test]
	fn test_exclude_system_version() {
		assert!(is_commit_excluded(KeyTag::SystemVersion));
	}

	#[test]
	fn test_exclude_transaction_version() {}

	#[test]
	fn test_exclude_ring_buffer_metadata() {
		assert!(is_commit_excluded(KeyTag::RingBufferMetadata));
	}

	#[test]
	fn test_exclude_index() {
		assert!(is_commit_excluded(KeyTag::Index));
	}

	#[test]
	fn test_include_namespace() {
		assert!(!is_commit_excluded(KeyTag::Namespace));
	}

	#[test]
	fn test_include_table() {
		assert!(!is_commit_excluded(KeyTag::Table));
	}

	#[test]
	fn test_include_row() {
		assert!(!is_commit_excluded(KeyTag::Row));
	}

	#[test]
	fn test_include_series_row() {
		// Series rows rode into the log under KeyTag::Row, so their own kind must keep them there.
		assert!(!is_commit_excluded(KeyTag::SeriesRow));
	}

	#[test]
	fn test_include_partitioned_row() {
		assert!(!is_commit_excluded(KeyTag::PartitionedRow));
	}

	#[test]
	fn test_include_partition() {
		assert!(!is_commit_excluded(KeyTag::Partition));
	}

	#[test]
	fn test_include_namespace_table() {
		assert!(!is_commit_excluded(KeyTag::NamespaceTable));
	}

	#[test]
	fn test_include_columns() {
		assert!(!is_commit_excluded(KeyTag::Columns));
	}

	#[test]
	fn test_include_column() {
		assert!(!is_commit_excluded(KeyTag::Column));
	}

	#[test]
	fn test_include_column_property() {
		assert!(!is_commit_excluded(KeyTag::ColumnProperty));
	}

	#[test]
	fn test_include_index_entry() {
		assert!(!is_commit_excluded(KeyTag::IndexEntry));
	}

	#[test]
	fn test_include_view() {
		assert!(!is_commit_excluded(KeyTag::View));
	}

	#[test]
	fn test_include_namespace_view() {
		assert!(!is_commit_excluded(KeyTag::NamespaceView));
	}

	#[test]
	fn test_include_primary_key() {
		assert!(!is_commit_excluded(KeyTag::PrimaryKey));
	}

	#[test]
	fn test_include_ring_buffer() {
		assert!(!is_commit_excluded(KeyTag::RingBuffer));
	}

	#[test]
	fn test_include_namespace_ring_buffer() {
		assert!(!is_commit_excluded(KeyTag::NamespaceRingBuffer));
	}

	#[test]
	fn test_include_queue() {
		assert!(!is_commit_excluded(KeyTag::Queue));
	}

	#[test]
	fn test_include_namespace_queue() {
		assert!(!is_commit_excluded(KeyTag::NamespaceQueue));
	}

	#[test]
	fn test_include_queue_deduplication() {
		assert!(!is_commit_excluded(KeyTag::QueueDeduplication));
	}

	#[test]
	fn test_exclude_queue_partition() {
		assert!(is_commit_excluded(KeyTag::QueuePartition));
	}

	#[test]
	fn test_exclude_queue_item_state() {
		assert!(is_commit_excluded(KeyTag::QueueItemState));
	}

	#[test]
	fn test_exclude_queue_due() {
		assert!(is_commit_excluded(KeyTag::QueueDue));
	}

	#[test]
	fn test_exclude_queue_key_active() {
		assert!(is_commit_excluded(KeyTag::QueueKeyActive));
	}

	#[test]
	fn test_include_queue_attempt() {
		// Attempt records are the durable audit trail of what a worker reported, not internal
		// scheduling churn. Excluding them would make every ack invisible to subscribers and
		// to any downstream view built on effect outcomes.
		assert!(!is_commit_excluded(KeyTag::QueueAttempt));
	}

	#[test]
	fn test_include_operator_retention() {
		assert!(!is_commit_excluded(KeyTag::OperatorRetention));
	}

	#[test]
	fn test_include_flow() {
		assert!(!is_commit_excluded(KeyTag::Flow));
	}

	#[test]
	fn test_include_namespace_flow() {
		assert!(!is_commit_excluded(KeyTag::NamespaceFlow));
	}

	#[test]
	fn test_include_operator() {
		assert!(!is_commit_excluded(KeyTag::Operator));
	}

	#[test]
	fn test_include_operator_by_flow() {
		assert!(!is_commit_excluded(KeyTag::OperatorByFlow));
	}

	#[test]
	fn test_include_flow_edge() {
		assert!(!is_commit_excluded(KeyTag::FlowEdge));
	}

	#[test]
	fn test_include_flow_edge_by_flow() {
		assert!(!is_commit_excluded(KeyTag::FlowEdgeByFlow));
	}

	#[test]
	fn test_include_dictionary() {
		assert!(!is_commit_excluded(KeyTag::Dictionary));
	}

	#[test]
	fn test_include_dictionary_entry() {
		assert!(!is_commit_excluded(KeyTag::DictionaryEntry));
	}

	#[test]
	fn test_include_dictionary_entry_index() {
		assert!(!is_commit_excluded(KeyTag::DictionaryEntryIndex));
	}

	#[test]
	fn test_include_namespace_dictionary() {
		assert!(!is_commit_excluded(KeyTag::NamespaceDictionary));
	}

	#[test]
	fn test_include_handler() {
		assert!(!is_commit_excluded(KeyTag::Handler));
	}

	#[test]
	fn test_include_namespace_handler() {
		assert!(!is_commit_excluded(KeyTag::NamespaceHandler));
	}

	#[test]
	fn test_include_variant_handler() {
		assert!(!is_commit_excluded(KeyTag::VariantHandler));
	}

	#[test]
	fn test_include_shape() {
		assert!(!is_commit_excluded(KeyTag::RowShape));
	}

	#[test]
	fn test_include_sum_type() {
		assert!(!is_commit_excluded(KeyTag::SumType));
	}

	#[test]
	fn test_include_namespace_sum_type() {
		assert!(!is_commit_excluded(KeyTag::NamespaceSumType));
	}

	#[test]
	fn test_include_shape_field() {
		assert!(!is_commit_excluded(KeyTag::RowShapeField));
	}

	#[test]
	fn test_include_series() {
		assert!(!is_commit_excluded(KeyTag::Series));
	}

	#[test]
	fn test_include_namespace_series() {
		assert!(!is_commit_excluded(KeyTag::NamespaceSeries));
	}

	#[test]
	fn test_include_series_partition_metadata() {
		assert!(!is_commit_excluded(KeyTag::SeriesPartitionMetadata));
	}

	#[test]
	fn test_include_identity() {
		assert!(!is_commit_excluded(KeyTag::Identity));
	}

	#[test]
	fn test_include_role() {
		assert!(!is_commit_excluded(KeyTag::Role));
	}

	#[test]
	fn test_include_granted_role() {
		assert!(!is_commit_excluded(KeyTag::GrantedRole));
	}

	#[test]
	fn test_include_identity_attribute() {
		assert!(!is_commit_excluded(KeyTag::IdentityAttribute));
	}

	#[test]
	fn test_include_identity_attribute_value() {
		assert!(!is_commit_excluded(KeyTag::IdentityAttributeValue));
	}

	#[test]
	fn test_include_authentication() {
		assert!(!is_commit_excluded(KeyTag::Authentication));
	}

	#[test]
	fn test_include_authentication_method() {
		assert!(!is_commit_excluded(KeyTag::AuthenticationMethod));
	}

	#[test]
	fn test_include_policy() {
		assert!(!is_commit_excluded(KeyTag::Policy));
	}

	#[test]
	fn test_include_policy_op() {
		assert!(!is_commit_excluded(KeyTag::PolicyOp));
	}

	#[test]
	fn test_include_migration() {
		assert!(!is_commit_excluded(KeyTag::Migration));
	}

	#[test]
	fn test_include_migration_event() {
		assert!(!is_commit_excluded(KeyTag::MigrationEvent));
	}

	#[test]
	fn test_exclude_flow_version() {
		assert!(is_commit_excluded(KeyTag::FlowVersion));
	}

	#[test]
	fn test_exclude_config() {
		assert!(is_commit_excluded(KeyTag::ConfigStorage));
	}

	#[test]
	fn test_exclude_version_epoch() {
		assert!(is_commit_excluded(KeyTag::VersionEpoch));
	}
}

#[cfg(test)]
mod layer_tests {
	use reifydb_value::util::cowvec::CowVec;

	use super::*;
	use crate::{
		interface::catalog::{id::TableId, storage::StorageId},
		key::row::RowKey,
	};

	fn key(row: u64) -> TaggedKey {
		TaggedKey::from(RowKey {
			storage: StorageId::table(TableId(1)),
			row: RowNumber(row),
		})
	}

	fn set(row: u64, byte: u8, pre: SetPre) -> Delta {
		Delta::Set {
			key: key(row),
			bytes: EncodedBytes(CowVec::new(vec![byte])),
			pre,
		}
	}

	fn version() -> ChangeVersion {
		ChangeVersion {
			commit: CommitVersion(50),
			source: SourceVersion(30),
		}
	}

	#[test]
	fn a_layered_entry_tags_each_delta_with_its_layer_source_and_puts_store_only_keys_in_the_last_layer() {
		// A delta tagged with the wrong layer hands a reader the row at another source; a dropped store-only
		// key loses a change.
		let layers = vec![
			CommitLayer {
				source: SourceVersion(10),
				deltas: vec![set(1, 1, SetPre::Absent)],
			},
			CommitLayer {
				source: SourceVersion(30),
				deltas: vec![set(1, 2, SetPre::Present(EncodedBytes(CowVec::new(vec![1]))))],
			},
		];
		let store = vec![set(1, 2, SetPre::Absent), set(9, 7, SetPre::Absent)];

		let entry = CommitEntry::build(version(), Lsn::new(1), DateTime::default(), store, layers);

		assert_eq!(entry.deltas.len(), 3, "both layer writes of row 1 and the store-only row 9");
		assert_eq!(entry.source_at(0), SourceVersion(10));
		assert_eq!(entry.source_at(1), SourceVersion(30));
		assert_eq!(entry.deltas[2].key(), &key(9), "a key no layer holds rides in the last layer");
		assert_eq!(entry.source_at(2), SourceVersion(30));
	}

	#[test]
	fn an_entry_without_layers_keeps_the_store_deltas_at_the_entry_source() {
		// A plain commit must lift exactly as before layers existed.
		let store = vec![set(1, 2, SetPre::Absent), set(9, 7, SetPre::Absent)];

		let entry = CommitEntry::build(version(), Lsn::new(1), DateTime::default(), store.clone(), Vec::new());

		assert_eq!(entry.deltas, store);
		assert!(entry.layers.is_empty());
		assert_eq!(entry.source_at(1), SourceVersion(30));
	}
}
