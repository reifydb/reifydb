// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::take,
	sync::Arc,
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, SourceVersion},
	delta::{Delta, RemoveVisibility, SetPre},
	event::transaction::PostCommitEvent,
	interface::{
		catalog::id::{QueueId, ViewId},
		operator::{OperatorSection, OperatorSlice},
	},
	key::any::TaggedKey,
	marker::{Marker, MarkerSink},
	metrics::heap::HeapSize,
	return_internal_error,
	util::ioc::IocContainer,
	wal::{CommitLayer, CommitRecord},
};
#[cfg(reifydb_assertions)]
use reifydb_core::{interface::catalog::storage::StorageId, key::tag::KeyTag};
use reifydb_runtime::context::{clock::Clock, transaction::TransactionContext};
#[cfg(reifydb_assertions)]
use reifydb_value::util::hex::display as hex_display;
use reifydb_value::{byte_size::ByteSize, error::ValueError, reifydb_assertions, util::cowvec::CowVec};
use reifydb_wal::{lsn::Lsn, sync::WalMode};
use tracing::instrument;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalCatalogChanges},
	delta::{finalize_layers, optimize_deltas, persistent_layers, raw_layers},
	error::TransactionError,
	interceptor::{
		chain::InterceptorChain as Chain,
		transaction::{PostCommitContext, PostCommitInterceptor},
	},
	marker::PendingMarkers,
	multi::{
		pending::{ENTRY_OVERHEAD, MAX_BATCH_ENTRIES, MAX_BATCH_SIZE},
		transaction::MultiTransaction,
		types::DeltaEntry,
	},
	wal::check_record_size,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
	pub version: CommitVersion,
	pub lsn: Option<Lsn>,
}

enum DeferredFlowWrite {
	Set {
		key: TaggedKey,
		bytes: EncodedBytes,
	},
	Remove {
		key: TaggedKey,
		visibility: RemoveVisibility,
	},
}

struct ResolvedWrites {
	deltas: Vec<Delta>,
	layers: Vec<CommitLayer>,
	encoded: Option<Vec<u8>>,
}

pub struct DeferredFlowCommit {
	multi: MultiTransaction,
	id: TransactionId,
	read_version: CommitVersion,
	read_registered: bool,
	source: Option<SourceVersion>,
	layer_marks: Vec<(SourceVersion, usize)>,
	writes: Vec<DeferredFlowWrite>,
	owned_queues: BTreeSet<QueueId>,
	operator: OperatorSection,
	row_changes: Vec<RowChange>,
	post_commit: Chain<dyn PostCommitInterceptor + Send + Sync>,
	marker_source: Option<(IocContainer, Clock)>,
	pending_markers: Option<Box<PendingMarkers>>,
	count: u64,
	size: ByteSize,
}

impl DeferredFlowCommit {
	pub fn new(
		multi: MultiTransaction,
		post_commit: Chain<dyn PostCommitInterceptor + Send + Sync>,
		marker_source: Option<(IocContainer, Clock)>,
	) -> Result<Self, ValueError> {
		let oracle = multi.tm.oracle().clone();
		let read_version = oracle.query.register_in_flight_with(|| oracle.version())?;
		let id = multi.generate_transaction_id();
		Ok(Self {
			multi,
			id,
			read_version,
			read_registered: true,
			source: None,
			layer_marks: Vec::new(),
			writes: Vec::new(),
			owned_queues: BTreeSet::new(),
			operator: OperatorSection::default(),
			row_changes: Vec::new(),
			post_commit,
			marker_source,
			pending_markers: None,
			count: 0,
			size: ByteSize::ZERO,
		})
	}

	pub fn get(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<EncodedBytes>>, ValueError> {
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		let ctx = TransactionContext::system(self.multi.runtime_context());
		Ok(self.multi
			.get(&ctx, keys, self.read_version)?
			.into_iter()
			.map(|found| found.map(|row| row.bytes))
			.collect())
	}

	pub fn stamp_source(&mut self, source: SourceVersion) {
		self.source = Some(source);
	}

	pub fn begin_layer(&mut self, source: SourceVersion) {
		self.layer_marks.push((source, self.writes.len()));
		self.source = Some(source);
	}

	pub fn own_queues(&mut self, queues: impl IntoIterator<Item = QueueId>) {
		self.owned_queues.extend(queues);
	}

	pub fn push_operator_slice(&mut self, slice: OperatorSlice) {
		self.operator.slices.push(slice);
	}

	pub fn set(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<(), ValueError> {
		let size =
			entries.iter().fold(ByteSize::ZERO, |total, (key, bytes)| total + entry_size(key, bytes.len()));
		self.admit(entries.iter().map(|(key, _)| key), entries.len() as u64, size)?;
		self.writes.extend(entries.into_iter().map(|(key, bytes)| DeferredFlowWrite::Set {
			key,
			bytes,
		}));
		Ok(())
	}

	pub fn remove(&mut self, entries: Vec<(TaggedKey, RemoveVisibility)>) -> Result<(), ValueError> {
		let size = entries.iter().fold(ByteSize::ZERO, |total, (key, _)| total + entry_size(key, 0));
		self.admit(entries.iter().map(|(key, _)| key), entries.len() as u64, size)?;
		self.writes.extend(entries.into_iter().map(|(key, visibility)| DeferredFlowWrite::Remove {
			key,
			visibility,
		}));
		Ok(())
	}

	pub fn track_row_change(&mut self, changes: &[RowChange]) {
		self.row_changes.extend_from_slice(changes);
	}

	pub fn attach_view_markers(&mut self, view: ViewId, markers: Vec<Marker>) -> Result<(), ValueError> {
		if self.pending_markers.is_none() {
			let Some((ioc, clock)) = self.marker_source.as_ref() else {
				return Ok(());
			};
			let Some(sink) = ioc.try_resolve::<Arc<dyn MarkerSink>>() else {
				return Ok(());
			};
			self.pending_markers = Some(Box::new(PendingMarkers::new(sink, clock.clone())));
		}
		let Some(pending) = self.pending_markers.as_deref_mut() else {
			return Ok(());
		};
		pending.attach_view(view, markers).map_err(ValueError::from)
	}

	#[instrument(name = "transaction::commit::deferred_flow", level = "debug", skip(self))]
	pub fn commit(mut self) -> Result<Committed, ValueError> {
		self.assert_owned_keys();
		let oracle = Arc::clone(self.multi.tm.oracle());
		if self.writes.is_empty() && self.operator.slices.is_empty() {
			oracle.query.mark_finished(self.read_version);
			self.read_registered = false;
			let row_changes = take(&mut self.row_changes);
			self.post_commit.execute(PostCommitContext::new(
				self.id,
				CommitVersion(0),
				TransactionalCatalogChanges::default(),
				row_changes,
			))?;
			return Ok(Committed {
				version: CommitVersion(0),
				lsn: None,
			});
		}
		let ResolvedWrites {
			deltas,
			layers,
			encoded,
		} = self.resolve_writes()?;
		let result = oracle.commit_deferred_flow(CowVec::new(deltas), encoded, self.source);
		oracle.query.mark_finished(self.read_version);
		self.read_registered = false;
		let (version, lsn, at, deltas) = result?;
		reifydb_assertions! {
			assert_ne!(
				version, 0,
				"a deferred flow commit reached version 0 with {} non-empty deltas; CommitVersion(0) is \
				 the empty/discarded sentinel callers read as 'nothing committed', so committing real \
				 deltas at it would silently drop them",
				deltas.len()
			);
		}
		let self_lease = oracle.leases.try_acquire(version, oracle.query.done_until()).ok();
		reifydb_assertions! {
			assert!(
				self_lease.is_some(),
				"self-version lease on freshly-committed version {} must succeed: it is the newest \
				 version so query.done_until() < it; failing means the historical-GC cutoff passed our \
				 own commit version before its post-commit hooks ran",
				version.0
			);
		}
		oracle.query.mark_finished(version);
		if oracle.wal.mode() == WalMode::Sync && oracle.wal.durable() < Some(lsn) {
			oracle.wal.sync().unwrap_or_else(|err| {
				panic!("commit {} failed to flush the commit log: {err}", version.0)
			});
		}
		if let Some(markers) = self.pending_markers.take() {
			markers.insert_into_sink(version);
		}
		oracle.done_commit(version);
		self.multi.event_bus.emit(PostCommitEvent::new(
			deltas,
			ChangeVersion {
				commit: version,
				source: self.source.unwrap_or(SourceVersion::from(version)),
			},
			lsn,
			at,
			CowVec::new(layers),
		));
		let row_changes = take(&mut self.row_changes);
		self.post_commit.execute(PostCommitContext::new(
			self.id,
			version,
			TransactionalCatalogChanges::default(),
			row_changes,
		))?;
		drop(self_lease);
		Ok(Committed {
			version,
			lsn: Some(lsn),
		})
	}

	fn assert_owned_keys(&self) {
		reifydb_assertions! {
			for write in &self.writes {
				let (DeferredFlowWrite::Set {
					key,
					..
				}
				| DeferredFlowWrite::Remove {
					key,
					..
				}) = write;
				let storage = key.flow_storage();
				let tag = key.kind();
				let owned = match storage {
					Some(StorageId::View(_)) => true,
					Some(StorageId::Queue(id)) => {
						self.owned_queues.contains(&id) && matches!(tag, KeyTag::Row | KeyTag::Partition)
					}
					_ => false,
				};
				assert!(
					owned,
					"flow commit carries a key outside its views: {tag:?} on {storage:?}; a checked user \
					 commit that wrote this key would keep a stale old value, because flow commits no \
					 longer mark the oracle"
				);
			}
		}
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn admit<'a>(
		&mut self,
		keys: impl Iterator<Item = &'a TaggedKey>,
		entries: u64,
		size: ByteSize,
	) -> Result<(), ValueError> {
		reifydb_assertions! {
			for key in keys {
				assert!(
					!matches!(key, TaggedKey::OperatorState(_)),
					"operator state must reach the operator store through the committer split, never \
					 the multi store: {}",
					hex_display(key.encode().as_ref())
				);
			}
		}
		let count = self.count + entries;
		let size = self.size + size;
		if count >= MAX_BATCH_ENTRIES || size >= MAX_BATCH_SIZE {
			return Err(TransactionError::TooLarge.into());
		}
		self.count = count;
		self.size = size;
		Ok(())
	}

	fn resolve_writes(&mut self) -> Result<ResolvedWrites, ValueError> {
		let writes = take(&mut self.writes);
		let mut listed: BTreeSet<&TaggedKey> = BTreeSet::new();
		let mut keys: Vec<TaggedKey> = Vec::new();
		for write in &writes {
			let read = match write {
				DeferredFlowWrite::Set {
					key,
					..
				} => Some(key),
				DeferredFlowWrite::Remove {
					key,
					visibility: RemoveVisibility::Announced | RemoveVisibility::Unobserved,
				} => Some(key),
				DeferredFlowWrite::Remove {
					visibility: RemoveVisibility::Silent,
					..
				} => None,
			};
			if let Some(key) = read
				&& listed.insert(key)
			{
				keys.push(key.clone());
			}
		}
		let mut stored: BTreeMap<TaggedKey, Option<EncodedBytes>> = BTreeMap::new();
		if !keys.is_empty() {
			let ctx = TransactionContext::system(self.multi.runtime_context());
			let found = self.multi.get(&ctx, &keys, self.read_version)?;
			stored.extend(keys
				.into_iter()
				.zip(found)
				.map(|(key, found)| (key, found.map(|row| row.bytes))));
		}
		let mut pending: BTreeMap<TaggedKey, Option<EncodedBytes>> = BTreeMap::new();
		let mut committed: BTreeMap<TaggedKey, EncodedBytes> = BTreeMap::new();
		let mut entries: Vec<DeltaEntry> = Vec::with_capacity(writes.len());
		for write in writes {
			let delta = match write {
				DeferredFlowWrite::Set {
					key,
					bytes,
				} => {
					pending.insert(key.clone(), Some(bytes.clone()));
					Delta::Set {
						key,
						bytes,
						pre: SetPre::Unknown,
					}
				}
				DeferredFlowWrite::Remove {
					key,
					visibility,
				} => {
					let in_batch = pending.get(&key).cloned();
					let in_store = stored.get(&key).cloned().flatten();
					if matches!(in_batch, Some(Some(_)))
						&& let Some(bytes) = &in_store
					{
						committed.insert(key.clone(), bytes.clone());
					}
					pending.insert(key.clone(), None);
					let seen = match in_batch {
						Some(value) => value,
						None => in_store,
					};
					match (visibility, seen) {
						(RemoveVisibility::Silent, _) | (_, None) => Delta::remove_silent(key),
						(RemoveVisibility::Announced, Some(bytes)) => {
							Delta::remove_announced(key, bytes)
						}
						(RemoveVisibility::Unobserved, Some(bytes)) => {
							Delta::remove_unobserved(key, bytes)
						}
					}
				}
			};
			entries.push(DeltaEntry {
				delta,
				version: self.read_version,
			});
		}
		let raw = raw_layers(&entries, &self.layer_marks, &BTreeSet::new());
		let mut deltas = optimize_deltas(entries.into_iter().map(|entry| entry.delta), &committed);
		for delta in &mut deltas {
			if let Delta::Set {
				key,
				pre,
				..
			} = delta && matches!(pre, SetPre::Unknown)
			{
				*pre = match stored.get(key) {
					Some(Some(bytes)) => SetPre::Present(bytes.clone()),
					Some(None) => SetPre::Absent,
					None => return_internal_error!(
						"a deferred flow commit set {:?} without reading its old value",
						key
					),
				};
			}
		}
		let layers = finalize_layers(&raw, &deltas);
		let oracle = self.multi.tm.oracle();
		let encoded = if oracle.wal.mode() == WalMode::Off {
			None
		} else {
			let store = &self.multi.store;
			let persistent: Vec<&Delta> =
				deltas.iter().filter(|delta| store.is_persistent(delta.key())).collect();
			let logged = persistent_layers(&layers, |key| store.is_persistent(key));
			let mut buf = Vec::new();
			CommitRecord::encode_refs(
				CommitVersion(0),
				SourceVersion(0),
				&persistent,
				&self.operator,
				&logged,
				&mut buf,
			);
			check_record_size(&oracle.wal, &buf)?;
			Some(buf)
		};
		Ok(ResolvedWrites {
			deltas,
			layers,
			encoded,
		})
	}
}

impl Drop for DeferredFlowCommit {
	fn drop(&mut self) {
		if self.read_registered {
			self.multi.tm.oracle().query.mark_finished(self.read_version);
		}
	}
}

fn entry_size(key: &TaggedKey, bytes: usize) -> ByteSize {
	ByteSize::from_bytes((ENTRY_OVERHEAD + key.heap_size() + bytes) as u64)
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			id::{QueueId, ViewId},
			object::ObjectId,
			storage::StorageId,
		},
		key::{
			any::TaggedKey,
			partition::PartitionKey,
			ringbuffer::RingBufferMetadataKey,
			row::{PartitionedRowKey, PartitionedSortedViewRowKey, RowKey, SortedViewRowKey},
			series::{PartitionedSeriesRowKey, SeriesPartitionMetadataKey, SeriesRowKey},
			sort_run::SortRun,
		},
	};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{partition::Partition, row_number::RowNumber},
	};

	use super::DeferredFlowCommit;
	use crate::{interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction};

	fn batch() -> DeferredFlowCommit {
		DeferredFlowCommit::new(MultiTransaction::testing(), Interceptors::new().post_commit, None)
			.expect("begin a deferred flow commit")
	}

	fn bytes() -> EncodedBytes {
		EncodedBytes(CowVec::new(vec![1]))
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "flow commit carries a key outside its views")]
	fn a_flow_commit_carrying_a_table_row_key_is_refused() {
		// a table row in a flow commit would leave a checked user commit trusting a stale old value
		let key = TaggedKey::from(RowKey {
			storage: StorageId::table(1),
			row: RowNumber(1),
		});

		let mut commit = batch();
		commit.set(vec![(key, bytes())]).unwrap();
		commit.commit().unwrap();
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "flow commit carries a key outside its views")]
	fn a_flow_commit_carrying_a_row_of_a_queue_it_does_not_own_is_refused() {
		// a guard that takes any queue lets a flow write another flow's queue unmarked
		let key = TaggedKey::from(RowKey {
			storage: StorageId::queue(2),
			row: RowNumber(1),
		});

		let mut commit = batch();
		commit.own_queues([QueueId(1)]);
		commit.set(vec![(key, bytes())]).unwrap();
		commit.commit().unwrap();
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "flow commit carries a key outside its views")]
	fn a_flow_commit_carrying_ring_metadata_on_its_own_queue_is_refused() {
		// an owned queue admits only rows and partitions, otherwise any kind on it slips past the guard
		let key = TaggedKey::from(RingBufferMetadataKey::new(StorageId::queue(1)));

		let mut commit = batch();
		commit.own_queues([QueueId(1)]);
		commit.set(vec![(key, bytes())]).unwrap();
		commit.commit().unwrap();
	}

	#[test]
	fn a_flow_commit_of_view_keys_and_owned_queue_keys_commits() {
		// every key kind a flow writes must pass the guard, otherwise real flow commits stop
		let view = StorageId::view(ViewId(1));
		let keys: Vec<TaggedKey> = vec![
			RowKey {
				storage: view,
				row: RowNumber(1),
			}
			.into(),
			PartitionedRowKey::new(view, Partition(1), RowNumber(1)).into(),
			SortedViewRowKey::new(view, SortRun::new([0x10u8]), RowNumber(1)).into(),
			PartitionedSortedViewRowKey::new(view, Partition(1), SortRun::new([0x10u8]), RowNumber(1))
				.into(),
			SeriesRowKey {
				storage: view,
				variant_tag: None,
				key: 0,
				sequence: 0,
			}
			.into(),
			PartitionedSeriesRowKey::new(view, Partition(1), None, 0, 0).into(),
			PartitionKey::new(ObjectId::View(ViewId(1)), Partition(1)).into(),
			RingBufferMetadataKey::new(view).into(),
			SeriesPartitionMetadataKey::new(view, Partition(1)).into(),
			RowKey {
				storage: StorageId::queue(QueueId(1)),
				row: RowNumber(1),
			}
			.into(),
			PartitionKey::new(ObjectId::Queue(QueueId(1)), Partition(1)).into(),
		];

		let mut commit = batch();
		commit.own_queues([QueueId(1)]);
		commit.set(keys.into_iter().map(|key| (key, bytes())).collect()).unwrap();
		let committed =
			commit.commit().expect("a commit of view keys and owned queue keys must pass the guard");

		assert!(committed.version > CommitVersion(0), "the commit must take a version");
	}
}
