// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	slice,
};

use arrow_array::RecordBatch;
use indexmap::IndexSet;
use reifydb_codec::{
	key::{encode_u64_asc, encoded::EncodedKey},
	row::{
		bytes::EncodedBytes,
		operator::state::{decode, encode},
		pod::EncodedPodRow,
		ringbuffer::EncodedRingBufferRow,
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	common::ChangeVersion,
	interface::{
		catalog::{
			flow::OperatorId,
			ringbuffer::{
				EvictionVisibility, RingBufferMetadata, decode_ringbuffer_metadata,
				encode_ringbuffer_metadata,
			},
			storage::StorageId,
			view::View,
		},
		change::{Change, Diff},
		flow::OperatorCapability,
		resolved::ResolvedView,
	},
	internal_err,
	key::{
		operator::{
			keyspace::ringbuffer::{
				PartitionedRingbufferEntry, PartitionedRingbufferEntryKey, PartitionedRingbufferMeta,
				PartitionedRingbufferMetaKey, PartitionedRingbufferTtlArm,
				PartitionedRingbufferTtlArmKey, RingbufferEntry, RingbufferEntryKey, RingbufferMeta,
				RingbufferMetaKey, RingbufferNextRow, RingbufferNextRowKey, RingbufferTtlArm,
				RingbufferTtlArmKey,
			},
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
		},
		ringbuffer::RingBufferMetadataKey,
		row::{PartitionedRowKey, RowKey},
		typed::direction::Asc,
	},
	partition::partition_col_indices,
	ring::{MetadataWrite, Ring, RingStore, oldest},
	row::row_shape_from_columns,
	state::{timer::TimerKind, typed::typed_key},
	value::batch::from_encoded_bytes,
};
use reifydb_flow::{
	operator::{
		sink::{
			SourceRowEncoder, coerce_columns,
			partition::{ensure_partition_unchanged, partition_batch, resolve_partitions_flow},
			shape_field_columns,
			table::{
				dictionary_encode_view_columns, dictionary_lookup_partition_columns,
				dictionary_lookup_view_columns,
			},
		},
		source::decode_change_keeping,
	},
	txn::{Batch, Emit, Get},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		duration::Duration,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{created_at, require_row_numbers},
	},
};

use super::DurableSink;
use crate::{
	error::FlowStateError,
	operator::join::column::JoinedColumnsBuilder,
	timer::{Timer, extension::TimerExtension},
	transaction::{FlowTransaction, deferred::DeferredTransaction, state::StateExtension},
};

const EXPIRY_WALK: usize = 64;

pub struct SinkRingBufferViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	shape: RowShape,
	storage: StorageId,
	capacity: u64,
	ttl: Option<Duration>,
	ttl_eviction: EvictionVisibility,
	partition_indices: Vec<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
	runtime_context: RuntimeContext,
}

struct SinkRing<'a> {
	op: &'a SinkRingBufferViewOperator,
	txn: &'a mut DeferredTransaction,
}

impl RingStore for SinkRing<'_> {
	fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>> {
		self.op.read_metadata(self.txn, partition)
	}

	fn reserve(&mut self, capacity: u64) -> Result<u64> {
		let base = self.op.read_next_row(self.txn)?;
		self.op.write_next_row(self.txn, base + capacity)?;
		Ok(base)
	}

	fn slots(&mut self, partition: Option<Partition>, rows: &[RowNumber]) -> Result<Vec<Option<EncodedBytes>>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<EncodedKey> = rows.iter().map(|&row| self.op.rb_key(row, partition)).collect();
		self.txn.get(&keys)
	}
}

struct Placed {
	row_idx: usize,
	partition: Option<Partition>,
	row: RowNumber,
	evicted: Option<EncodedBytes>,
}

impl SinkRingBufferViewOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		operator: OperatorId,
		view: ResolvedView,
		capacity: u64,
		ttl: Option<Duration>,
		partition_by: Vec<String>,
		runtime_context: RuntimeContext,
	) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		let storage = view.def().storage_id();
		let shape = row_shape_from_columns(RowFamily::RingBuffer, view.def().columns());
		let View::RingBuffer(_) = view.def() else {
			panic!(
				"ring buffer view sink {:?} was built over a view that is not ring buffer backed",
				operator
			)
		};
		Self {
			operator,
			view,
			shape,
			storage,
			capacity,
			ttl,
			ttl_eviction: EvictionVisibility::Silent,
			partition_indices,
			verified_partitions: HashMap::new(),
			runtime_context,
		}
	}

	pub fn with_ttl_eviction(mut self, ttl_eviction: EvictionVisibility) -> Self {
		self.ttl_eviction = ttl_eviction;
		self
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	#[inline]
	fn rb_key(&self, rn: RowNumber, partition: Option<Partition>) -> EncodedKey {
		match partition {
			Some(partition) => PartitionedRowKey::encoded(self.storage, partition, rn),
			None => RowKey::encoded(self.storage, rn),
		}
	}

	fn meta_key(&self, partition: Option<Partition>) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRingbufferMeta>(
				GroupId::ROOT,
				&PartitionedRingbufferMetaKey {
					partition: Asc(partition),
				},
			),
			None => typed_key::<RingbufferMeta>(GroupId::ROOT, &RingbufferMetaKey {}),
		}
	}

	fn read_meta_mirror(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		let key = self.meta_key(partition);
		let Some(row) = self.state_get(txn, slice::from_ref(&key))?.pop().flatten() else {
			return Ok(None);
		};
		Ok(Some(decode_ringbuffer_metadata(&EncodedPodRow::new(row.body()))?))
	}

	fn write_meta_mirror(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = self.meta_key(partition);
		let row = EncodedPodRow::new(encode_ringbuffer_metadata(metadata).as_slice());
		self.state_set(txn, vec![(key, row)])
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn assert_mirrors_mvcc(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		mvcc: &EncodedKey,
	) -> Result<()> {
		reifydb_assertions! {
			let mirrored = self.read_meta_mirror(txn, partition)?;
			let stored = txn.get(slice::from_ref(mvcc))?.pop().flatten().map(|row| decode_ringbuffer_metadata(EncodedPodRow::view(&row))).transpose()?;
			assert_eq!(
				mirrored, stored,
				"the ringbuffer metadata mirror and its mvcc row disagree right after a write; the \
				 mirror is what replay reads, so any drift hands catch-up a different slot range than the \
				 live run assigned and the operator state forward map diverges silently"
			);
		}
		Ok(())
	}

	fn read_metadata(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		if let Some(metadata) = self.read_meta_mirror(txn, partition)? {
			return Ok(Some(metadata));
		}
		let key = RingBufferMetadataKey::encoded_of(self.storage, partition);
		let Some(row) = txn.get(slice::from_ref(&key))?.pop().flatten() else {
			return Ok(None);
		};
		let metadata = decode_ringbuffer_metadata(EncodedPodRow::view(&row))?;
		self.write_meta_mirror(txn, partition, &metadata)?;
		Ok(Some(metadata))
	}

	fn write_metadata(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_of(self.storage, partition);
		txn.set_one(&key, encode_ringbuffer_metadata(metadata).into_bytes())?;
		self.write_meta_mirror(txn, partition, metadata)?;
		self.assert_mirrors_mvcc(txn, partition, &key)
	}

	fn remove_metadata(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_of(self.storage, partition);
		txn.remove_one(&key)?;
		self.state_remove(txn, &[self.meta_key(partition)])?;
		self.assert_mirrors_mvcc(txn, partition, &key)
	}

	fn save_ring(&self, txn: &mut DeferredTransaction, ring: &Ring) -> Result<()> {
		for write in ring.writes() {
			match write {
				MetadataWrite::Save(partition, metadata) => {
					self.write_metadata(txn, partition, &metadata)?
				}
				MetadataWrite::Remove(partition) => self.remove_metadata(txn, partition)?,
			}
		}
		Ok(())
	}

	fn next_row_key(&self) -> GroupStateKey {
		typed_key::<RingbufferNextRow>(GroupId::ROOT, &RingbufferNextRowKey {})
	}

	fn read_next_row(&self, txn: &mut DeferredTransaction) -> Result<u64> {
		match self.state_get(txn, &[self.next_row_key()])?.pop().flatten() {
			Some(row) => decode_u64(&row, "RingBufferNextRow"),
			None => Ok(1),
		}
	}

	fn write_next_row(&self, txn: &mut DeferredTransaction, next_row: u64) -> Result<()> {
		self.state_set(txn, vec![(self.next_row_key(), encode_u64(next_row, "RingBufferNextRow")?)])
	}

	fn forward_key(&self, source_rn: RowNumber) -> GroupStateKey {
		OperatorStateKey::inner_encoded(
			GroupId::ROOT,
			KeyspaceId::RINGBUFFER_FORWARD,
			encode_u64_asc(source_rn.0),
		)
	}

	fn forwards(&self, txn: &mut DeferredTransaction, source_rns: &[RowNumber]) -> Result<Vec<Option<RowNumber>>> {
		let keys: Vec<GroupStateKey> = source_rns.iter().map(|rn| self.forward_key(*rn)).collect();
		self.state_get(txn, &keys)?
			.into_iter()
			.map(|row| row.map(|row| decode_u64(&row, "RingBufferForward").map(RowNumber)).transpose())
			.collect()
	}

	fn row_entry_key(&self, partition: Option<Partition>, storage_rn: RowNumber) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRingbufferEntry>(
				GroupId::ROOT,
				&PartitionedRingbufferEntryKey {
					partition: Asc(partition),
					row: Asc(storage_rn),
				},
			),
			None => typed_key::<RingbufferEntry>(
				GroupId::ROOT,
				&RingbufferEntryKey {
					row: Asc(storage_rn),
				},
			),
		}
	}

	fn arm_key(&self, partition: Option<Partition>) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRingbufferTtlArm>(
				GroupId::ROOT,
				&PartitionedRingbufferTtlArmKey {
					partition: Asc(partition),
				},
			),
			None => typed_key::<RingbufferTtlArm>(GroupId::ROOT, &RingbufferTtlArmKey {}),
		}
	}

	fn expires_at(&self, time: Option<DateTime>) -> Option<i64> {
		let ttl = self.ttl?;
		let time = time?;
		let ttl_ms = ttl.milliseconds().ok()?;
		Some(time.to_millis().saturating_add(ttl_ms))
	}

	fn row_sources(
		&self,
		txn: &mut DeferredTransaction,
		slots: &[(Option<Partition>, RowNumber)],
	) -> Result<Vec<Option<RowNumber>>> {
		let keys: Vec<GroupStateKey> =
			slots.iter().map(|(partition, rn)| self.row_entry_key(*partition, *rn)).collect();
		self.state_get(txn, &keys)?
			.into_iter()
			.map(|row| row.map(|row| decode_u64(&row, "RingBufferRowEntry").map(RowNumber)).transpose())
			.collect()
	}

	fn release_slots(
		&self,
		txn: &mut DeferredTransaction,
		slots: &[(Option<Partition>, RowNumber)],
		removes: &mut Vec<GroupStateKey>,
	) -> Result<()> {
		for (&(partition, rn), source) in slots.iter().zip(self.row_sources(txn, slots)?) {
			if let Some(source) = source {
				removes.push(self.forward_key(source));
			}
			removes.push(self.row_entry_key(partition, rn));
		}
		Ok(())
	}
}

fn encode_u64(value: u64, state: &'static str) -> Result<EncodedPodRow> {
	encode(&value).map_err(|e| {
		Error::from(FlowStateError::Encode {
			state,
			cause: e.to_string(),
		})
	})
}

fn decode_u64(row: &EncodedPodRow, state: &'static str) -> Result<u64> {
	decode(row).map_err(|e| {
		Error::from(FlowStateError::Decode {
			state,
			cause: e.to_string(),
		})
	})
}

fn encode_i64(value: i64, state: &'static str) -> Result<EncodedPodRow> {
	encode(&value).map_err(|e| {
		Error::from(FlowStateError::Encode {
			state,
			cause: e.to_string(),
		})
	})
}

fn decode_i64(row: &EncodedPodRow, state: &'static str) -> Result<i64> {
	decode(row).map_err(|e| {
		Error::from(FlowStateError::Decode {
			state,
			cause: e.to_string(),
		})
	})
}

fn with_sequence(bytes: EncodedBytes, sequence: u64) -> EncodedBytes {
	let mut row = EncodedRingBufferRow::from(bytes).thaw();
	row.set_sequence(sequence);
	row.freeze().into_bytes()
}

impl SinkRingBufferViewOperator {
	fn state_get(
		&self,
		txn: &mut DeferredTransaction,
		keys: &[GroupStateKey],
	) -> Result<Vec<Option<EncodedPodRow>>> {
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		txn.state_get(self.operator, keys)
	}

	fn state_set(&self, txn: &mut DeferredTransaction, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		txn.state_set(self.operator, rows)
	}

	fn state_remove(&self, txn: &mut DeferredTransaction, keys: &[GroupStateKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		txn.state_remove(self.operator, keys)
	}
}

impl DurableSink for SinkRingBufferViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
		let change = decode_change_keeping(txn, change, self.view.def().columns())?;
		let resolved = self.view.clone();
		let view = resolved.def();
		let mut ring = Ring::new(self.capacity);
		let mut touched: IndexSet<Option<Partition>> = IndexSet::new();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_ringbuffer_insert(txn, view, &mut ring, post, &mut touched)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_ringbuffer_update(txn, view, pre, post, &mut touched)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_ringbuffer_remove(txn, view, &mut ring, pre, &mut touched)?,
			}
		}

		self.save_ring(txn, &ring)?;
		for &partition in &touched {
			self.sync_row_ttl_timer(txn, partition)?;
		}

		Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at))
	}

	fn on_timer(&mut self, txn: &mut DeferredTransaction, timer: Timer) -> Result<Option<Change>> {
		if timer.kind != TimerKind::RowTtl || self.ttl.is_none() {
			return Ok(None);
		}
		let partition = self.timer_partition(&timer.key)?;

		let evicted_any = self.evict_due(txn, partition, timer.due.to_millis())?;
		self.sync_row_ttl_timer(txn, partition)?;

		if evicted_any {
			let version = ChangeVersion::from(txn.version());
			return Ok(Some(Change::from_flow(self.operator, version, Vec::new(), timer.due)));
		}
		Ok(None)
	}
}

impl SinkRingBufferViewOperator {
	fn timer_key(&self, partition: Option<Partition>) -> EncodedKey {
		RingBufferMetadataKey::encoded_of(self.storage, partition)
	}

	fn timer_partition(&self, key: &EncodedKey) -> Result<Option<Partition>> {
		RingBufferMetadataKey::decode(key).and_then(|decoded| decoded.partition()).ok_or_else(|| {
			Error::from(FlowStateError::Decode {
				state: "RingBufferMetadataKey",
				cause: "a RowTtl timer key must decode as ring buffer metadata".to_string(),
			})
		})
	}

	fn read_armed(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<Option<i64>> {
		match self.state_get(txn, &[self.arm_key(partition)])?.pop().flatten() {
			Some(row) => Ok(Some(decode_i64(&row, "RingBufferTtlArm")?)),
			None => Ok(None),
		}
	}

	fn earliest_expiry(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<Option<i64>> {
		let mut store = SinkRing {
			op: self,
			txn,
		};
		let head = oldest(&mut store, partition, self.capacity)?;
		Ok(head.and_then(|(_, bytes)| self.expires_at(EncodedRingBufferRow::view(&bytes).time())))
	}

	fn sync_row_ttl_timer(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<()> {
		if self.ttl.is_none() {
			return Ok(());
		}
		let armed = self.read_armed(txn, partition)?;
		let earliest = self.earliest_expiry(txn, partition)?;
		if armed == earliest {
			return Ok(());
		}
		let key = self.timer_key(partition);
		if let Some(at) = armed {
			txn.disarm_timer(
				self.operator,
				&Timer {
					due: DateTime::from_millis(at),
					kind: TimerKind::RowTtl,
					key: key.clone(),
				},
			)?;
		}
		let arm_key = self.arm_key(partition);
		match earliest {
			Some(at) => {
				txn.arm_timer(
					self.operator,
					&Timer {
						due: DateTime::from_millis(at),
						kind: TimerKind::RowTtl,
						key,
					},
				)?;
				self.state_set(txn, vec![(arm_key, encode_i64(at, "RingBufferTtlArm")?)])
			}
			None => self.state_remove(txn, &[arm_key]),
		}
	}

	fn evict_due(&self, txn: &mut DeferredTransaction, partition: Option<Partition>, at: i64) -> Result<bool> {
		let Some(ttl_ms) = self.ttl.and_then(|ttl| ttl.milliseconds().ok()) else {
			return Ok(false);
		};
		let cutoff = DateTime::from_millis(at.saturating_sub(ttl_ms).saturating_add(1));
		let mut ring = Ring::new(self.capacity);
		let expired = {
			let mut store = SinkRing {
				op: self,
				txn: &mut *txn,
			};
			ring.expire(&mut store, partition, cutoff, EXPIRY_WALK)?
		};
		if expired.is_empty() {
			return Ok(false);
		}

		let slots: Vec<(Option<Partition>, RowNumber)> =
			expired.iter().map(|(rn, _)| (partition, *rn)).collect();
		let mut removes = Vec::with_capacity(slots.len() * 2);
		self.release_slots(txn, &slots, &mut removes)?;
		self.state_remove(txn, &removes)?;
		for (rn, _) in &expired {
			let key = self.rb_key(*rn, partition);
			match self.ttl_eviction {
				EvictionVisibility::Visible => txn.remove_one(&key)?,
				EvictionVisibility::Silent => txn.remove_unobserved(&key)?,
			}
		}
		self.save_ring(txn, &ring)?;
		Ok(true)
	}

	fn partition_groups(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		source: &RecordBatch,
	) -> Result<Vec<(Option<Partition>, Vec<usize>)>> {
		if !self.is_partitioned() {
			return Ok(vec![(None, (0..source.num_rows()).collect())]);
		}
		let parts = partition_batch(view, &self.partition_indices, source)?;
		resolve_partitions_flow(txn, self.storage.into(), &parts.distinct, &mut self.verified_partitions)?;
		let mut groups: Vec<(Option<Partition>, Vec<usize>)> =
			parts.distinct.iter().map(|(partition, _)| (Some(*partition), Vec::new())).collect();
		let index: HashMap<Partition, usize> =
			parts.distinct.iter().enumerate().map(|(group, (partition, _))| (*partition, group)).collect();
		for (row_idx, partition) in parts.rows.iter().enumerate() {
			groups[index[partition]].1.push(row_idx);
		}
		Ok(groups)
	}

	#[inline]
	fn apply_ringbuffer_insert(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		ring: &mut Ring,
		post: &RecordBatch,
		touched: &mut IndexSet<Option<Partition>>,
	) -> Result<()> {
		if post.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let field_columns = shape_field_columns(source, &self.shape);
		let encoder = SourceRowEncoder::new(source, &self.shape, &field_columns)?;
		let row_numbers = require_row_numbers(source)?;
		let encoded = encoder.encode_all()?;

		let mut placed: Vec<Placed> = Vec::with_capacity(source.num_rows());
		let mut sequences: Vec<u64> = Vec::with_capacity(source.num_rows());
		for (partition, rows) in self.partition_groups(txn, view, source)? {
			touched.insert(partition);
			let kept = &rows[rows.len().saturating_sub(self.capacity as usize)..];
			let mut store = SinkRing {
				op: self,
				txn: &mut *txn,
			};
			for (&row_idx, placement) in kept.iter().zip(ring.place(&mut store, partition, kept.len())?) {
				sequences.push(placement.sequence);
				placed.push(Placed {
					row_idx,
					partition,
					row: placement.row,
					evicted: placement.evicted,
				});
			}
		}

		let evicted_slots: Vec<(Option<Partition>, RowNumber)> = placed
			.iter()
			.filter(|placed| placed.evicted.is_some())
			.map(|placed| (placed.partition, placed.row))
			.collect();
		let mut removes = Vec::with_capacity(evicted_slots.len() * 2);
		self.release_slots(txn, &evicted_slots, &mut removes)?;
		self.state_remove(txn, &removes)?;

		let mut sets: Vec<(GroupStateKey, EncodedPodRow)> = Vec::with_capacity(placed.len() * 2);
		let mut row_keys: Vec<EncodedKey> = Vec::with_capacity(placed.len());
		let mut values: Vec<EncodedBytes> = Vec::with_capacity(placed.len());
		let mut fresh: Vec<usize> = Vec::new();
		let mut overwritten: Vec<usize> = Vec::new();
		let mut overwritten_rows: Vec<RowNumber> = Vec::new();
		let mut evicted_rows: Vec<EncodedBytes> = Vec::new();
		for (placed, sequence) in placed.into_iter().zip(sequences) {
			let source_rn = row_numbers[placed.row_idx];
			sets.push((self.forward_key(source_rn), encode_u64(placed.row.0, "RingBufferForward")?));
			sets.push((
				self.row_entry_key(placed.partition, placed.row),
				encode_u64(source_rn.0, "RingBufferRowEntry")?,
			));
			row_keys.push(self.rb_key(placed.row, placed.partition));
			values.push(with_sequence(encoded[placed.row_idx].clone(), sequence));
			match placed.evicted {
				Some(evicted) => {
					overwritten.push(placed.row_idx);
					overwritten_rows.push(placed.row);
					evicted_rows.push(evicted);
				}
				None => fresh.push(placed.row_idx),
			}
		}
		self.state_set(txn, sets)?;
		txn.set(row_keys, values)?;

		if !fresh.is_empty() {
			txn.emit(view.id(), Diff::insert(JoinedColumnsBuilder::retain_rows(&coerced, &fresh)?))?;
		}
		if !overwritten.is_empty() {
			txn.emit(
				view.id(),
				Diff::update(
					from_encoded_bytes(&self.shape, &overwritten_rows, &evicted_rows)?,
					JoinedColumnsBuilder::retain_rows(&coerced, &overwritten)?,
				),
			)?;
		}
		Ok(())
	}

	#[inline]
	fn apply_ringbuffer_update(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		pre: &RecordBatch,
		post: &RecordBatch,
		touched: &mut IndexSet<Option<Partition>>,
	) -> Result<()> {
		if pre.num_rows() != post.num_rows() {
			return internal_err!(
				"ringbuffer update has {} pre rows but {} post rows",
				pre.num_rows(),
				post.num_rows()
			);
		}
		if post.num_rows() == 0 {
			return Ok(());
		}
		let coerced_pre = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let coerced_post = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_pre = if self.is_partitioned() {
			dictionary_lookup_partition_columns(txn, view, &self.partition_indices, &coerced_pre)?
		} else {
			None
		};
		let dict_post = dictionary_encode_view_columns(txn, view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let pre_row_numbers = require_row_numbers(source_pre)?;
		let post_partitions: Option<Vec<Partition>> = if self.is_partitioned() {
			let pre_parts = partition_batch(view, &self.partition_indices, source_pre)?;
			let post_parts = partition_batch(view, &self.partition_indices, source_post)?;
			for (&pre_partition, &post_partition) in pre_parts.rows.iter().zip(&post_parts.rows) {
				ensure_partition_unchanged(self.storage.into(), pre_partition, post_partition)?;
			}
			resolve_partitions_flow(
				txn,
				self.storage.into(),
				&post_parts.distinct,
				&mut self.verified_partitions,
			)?;
			Some(post_parts.rows)
		} else {
			None
		};

		let mut applied: Vec<usize> = Vec::with_capacity(post.num_rows());
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(post.num_rows());
		for (row_idx, forward) in self.forwards(txn, pre_row_numbers)?.into_iter().enumerate() {
			let partition = post_partitions.as_ref().map(|partitions| partitions[row_idx]);
			touched.insert(partition);
			let Some(storage_rn) = forward else {
				continue;
			};
			keys.push(self.rb_key(storage_rn, partition));
			applied.push(row_idx);
		}
		let stored = txn.get(&keys)?;
		let mut created = created_at(source_post)?.to_vec();
		let mut sequences: Vec<u64> = Vec::with_capacity(applied.len());
		for (&row_idx, row) in applied.iter().zip(&stored) {
			let row = row.as_ref().map(EncodedRingBufferRow::view);
			if let (Some(stamp), Some(slot)) =
				(row.map(|row| row.created_at()).filter(|c| !c.is_epoch()), created.get_mut(row_idx))
			{
				*slot = stamp;
			}
			sequences.push(row.map_or(0, |row| row.sequence()));
		}
		let field_columns = shape_field_columns(source_post, &self.shape);
		let encoded = SourceRowEncoder::new(source_post, &self.shape, &field_columns)?
			.with_created_at(&created)
			.encode_all()?;
		for ((key, &row_idx), sequence) in keys.iter().zip(&applied).zip(sequences) {
			txn.set_one(key, with_sequence(encoded[row_idx].clone(), sequence))?;
		}
		if !applied.is_empty() {
			txn.emit(
				view.id(),
				Diff::update(
					JoinedColumnsBuilder::retain_rows(&coerced_pre, &applied)?,
					JoinedColumnsBuilder::retain_rows(&coerced_post, &applied)?,
				),
			)?;
		}
		Ok(())
	}

	#[inline]
	fn apply_ringbuffer_remove(
		&self,
		txn: &mut DeferredTransaction,
		view: &View,
		ring: &mut Ring,
		pre: &RecordBatch,
		touched: &mut IndexSet<Option<Partition>>,
	) -> Result<()> {
		if pre.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_lookup_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_numbers = require_row_numbers(&coerced)?;
		let partitions: Option<Vec<Partition>> = if self.is_partitioned() {
			Some(partition_batch(view, &self.partition_indices, source)?.rows)
		} else {
			None
		};
		let forwards = self.forwards(txn, row_numbers)?;
		let mut seen: HashSet<RowNumber> = HashSet::with_capacity(row_numbers.len());
		let mut found: Vec<(usize, RowNumber, Option<Partition>)> = Vec::with_capacity(row_numbers.len());
		let mut removes: Vec<GroupStateKey> = Vec::with_capacity(row_numbers.len() * 2);
		for (row_idx, (&source_rn, forward)) in row_numbers.iter().zip(forwards).enumerate() {
			let Some(storage_rn) = forward else {
				continue;
			};
			if !seen.insert(source_rn) {
				continue;
			}
			let partition = partitions.as_ref().map(|partitions| partitions[row_idx]);
			touched.insert(partition);
			removes.push(self.forward_key(source_rn));
			removes.push(self.row_entry_key(partition, storage_rn));
			found.push((row_idx, storage_rn, partition));
		}

		let mut grouped: Vec<(Option<Partition>, Vec<(usize, RowNumber)>)> = Vec::new();
		for (row_idx, storage_rn, partition) in found {
			match grouped.iter_mut().find(|(group, _)| *group == partition) {
				Some((_, rows)) => rows.push((row_idx, storage_rn)),
				None => grouped.push((partition, vec![(row_idx, storage_rn)])),
			}
		}
		let mut applied: Vec<usize> = Vec::with_capacity(row_numbers.len());
		for (partition, rows) in grouped {
			let storage_rns: Vec<RowNumber> = rows.iter().map(|(_, rn)| *rn).collect();
			let mut store = SinkRing {
				op: self,
				txn: &mut *txn,
			};
			for ((row_idx, storage_rn), removed) in
				rows.into_iter().zip(ring.remove(&mut store, partition, &storage_rns)?)
			{
				if removed.is_none() {
					continue;
				}
				txn.remove_one(&self.rb_key(storage_rn, partition))?;
				applied.push(row_idx);
			}
		}
		applied.sort_unstable();
		self.state_remove(txn, &removes)?;
		if !applied.is_empty() {
			txn.emit(view.id(), Diff::remove(JoinedColumnsBuilder::retain_rows(&coerced, &applied)?))?;
		}
		Ok(())
	}
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
	use std::{ops::Bound, sync::Arc};

	use arrow_array::{ArrayRef, UInt64Array};
	use reifydb_codec::key::encode_u128_asc;
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::CommitVersion,
		delta::RemoveVisibility,
		durability::Durability,
		interface::{
			catalog::{
				column::{Column as CatalogColumn, ColumnIndex},
				id::{ColumnId, NamespaceId, ViewId},
				namespace::Namespace,
				view::{RingBufferView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
		key::{any::TaggedKey, operator::state::GroupStateRange},
		value::{
			batch::batch,
			column::factory::{int4, utf8},
		},
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			column_view::ColumnView,
			constraint::TypeConstraint,
			container::temporal_array::datetime_array,
			datetime::DateTime,
			identity::IdentityId,
			system_columns::{SystemColumn, with_system_column},
			value_type::ValueType,
		},
	};

	use super::*;
	use crate::{
		operator::state::iter::StateIterator,
		transaction::{
			deferred::StorageCounts,
			mock::FlowTxn,
			scope::scoped_key,
			substrate::{apply_operator_state, classify_pending},
		},
	};

	const RB: StorageId = StorageId::View(ViewId(1));
	const T0: i64 = 1_000_000_000_000;
	const HOUR: i64 = 3_600 * 1_000_000_000;
	const AFTER: i64 = T0 + HOUR + 1_000_000_000;

	fn hour_ttl() -> Duration {
		Duration::from_hours(1).expect("one hour is representable")
	}

	fn view_def(partitioned: bool) -> View {
		let mut columns = Vec::new();
		if partitioned {
			columns.push(CatalogColumn {
				id: ColumnId(1),
				name: "base".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			});
		}
		columns.push(CatalogColumn {
			id: ColumnId(2),
			name: "n".to_string(),
			constraint: TypeConstraint::unconstrained(ValueType::Int4),
			properties: vec![],
			index: ColumnIndex(if partitioned {
				1
			} else {
				0
			}),
			auto_increment: false,
			dictionary_id: None,
		});
		View::RingBuffer(RingBufferView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "rb".to_string(),
			kind: ViewKind::Deferred,
			columns,
			primary_key: None,
			partition_by: if partitioned {
				vec!["base".to_string()]
			} else {
				Vec::new()
			},
			capacity: 100,
			sort: vec![],
		})
	}

	fn build_op(partitioned: bool, ttl: Option<Duration>) -> SinkRingBufferViewOperator {
		let view = view_def(partitioned);
		let resolved = ResolvedView::new(
			Fragment::internal("rb"),
			ResolvedNamespace::new(Fragment::internal("test"), Namespace::system()),
			view.clone(),
		);
		let partition_by = if partitioned {
			vec!["base".to_string()]
		} else {
			Vec::new()
		};
		SinkRingBufferViewOperator::new(
			OperatorId(1),
			resolved,
			100,
			ttl,
			partition_by,
			RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0))),
		)
	}

	fn deferred_txn(engine: &TestEngine) -> DeferredTransaction {
		engine.flow_txn().clock_millis(0).deferred()
	}

	fn commit_flow_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
		// Mirrors the committer split: state to the operator state store, everything else to the multi store.
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system(), Durability::Async).unwrap();
		for (key, pw) in pending.rows_ordered() {
			let key = TaggedKey::decode(key).unwrap();
			match pw {
				PendingWrite::Set(v) => cmd.set(vec![(key.clone(), v.clone())]).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced,
				} => cmd.remove(vec![(key.clone(), None)]).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved,
				} => cmd.remove_unobserved(&key).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Silent,
				} => cmd.remove_silent(&key).unwrap(),
			};
		}
		cmd.commit().unwrap();
		apply_operator_state(
			&engine.inner().operator_state(),
			&pending,
			&classify_pending(&engine.inner().operator_state(), &pending).unwrap(),
		);
	}

	fn columns_at(partitioned: bool, rows: &[(&str, i32)], first_source_rn: u64, time: i64) -> RecordBatch {
		let ns: Vec<i32> = rows.iter().map(|(_, n)| *n).collect();
		let rns: Vec<u64> = (0..rows.len() as u64).map(|i| first_source_rn + i).collect();
		let ts: Vec<DateTime> = rows.iter().map(|_| DateTime::from_nanos(time)).collect();
		let mut cols = Vec::new();
		if partitioned {
			let bases: Vec<String> = rows.iter().map(|(b, _)| b.to_string()).collect();
			cols.push(utf8("base", bases));
		}
		cols.push(int4("n", ns));
		let stamp = || -> ArrayRef { Arc::new(datetime_array(ts.clone())) };
		let system: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(rns))),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		];
		system.into_iter().fold(batch(cols).unwrap(), |columns, (column, array)| {
			with_system_column(columns, column, array).unwrap()
		})
	}

	fn insert(
		engine: &TestEngine,
		op: &mut SinkRingBufferViewOperator,
		partitioned: bool,
		rows: &[(&str, i32)],
		first_source_rn: u64,
	) -> CommitVersion {
		insert_at(engine, op, partitioned, rows, first_source_rn, T0)
	}

	fn insert_at(
		engine: &TestEngine,
		op: &mut SinkRingBufferViewOperator,
		partitioned: bool,
		rows: &[(&str, i32)],
		first_source_rn: u64,
		time: i64,
	) -> CommitVersion {
		let mut txn = deferred_txn(engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				OperatorId(1),
				ChangeVersion::from(CommitVersion(1)),
				vec![Diff::insert(columns_at(partitioned, rows, first_source_rn, time))],
				DateTime::from_nanos(time),
			),
		)
		.unwrap();
		commit_flow_pending(engine, &mut txn);
		engine.current_version().unwrap()
	}

	fn fire(
		engine: &TestEngine,
		op: &mut SinkRingBufferViewOperator,
		partition_values: &[Value],
		at: i64,
	) -> Option<Change> {
		// Stands in for the dispatcher. Eviction is decided by the timer's own instant, so
		// neither the operator nor the test needs a clock.
		let mut txn = deferred_txn(engine);
		let key = op.timer_key(partition_of_values(partition_values));
		let out = op
			.on_timer(
				&mut txn,
				Timer {
					due: DateTime::from_nanos(at),
					kind: TimerKind::RowTtl,
					key,
				},
			)
			.unwrap();
		commit_flow_pending(engine, &mut txn);
		out
	}

	fn partition_of_values(values: &[Value]) -> Option<Partition> {
		(!values.is_empty()).then(|| Partition::of(values))
	}

	fn keyspace_count(
		engine: &TestEngine,
		op: &SinkRingBufferViewOperator,
		keyspace: KeyspaceId,
		partition: Option<Partition>,
	) -> usize {
		let mut txn = deferred_txn(engine);
		let (start, end) = match partition {
			None => (Bound::Unbounded, Bound::Unbounded),
			Some(partition) => (
				Bound::Included(encode_u128_asc(partition.0).to_vec()),
				match partition.0.checked_add(1) {
					Some(next) => Bound::Excluded(encode_u128_asc(next).to_vec()),
					None => Bound::Unbounded,
				},
			),
		};
		let range = GroupStateRange::Keyspace {
			group: GroupId::ROOT,
			keyspace,
			start,
			end,
		};
		StateIterator::new(txn.range(range.for_operator(op.operator), RangeScope::All, 1024))
			.collect::<Result<Vec<_>>>()
			.unwrap()
			.len()
	}

	#[test]
	fn every_ringbuffer_state_key_lives_in_the_root_group_in_its_own_keyspace() {
		// A hand-rolled leading byte aliases a group-id varint, so only the root group is exempt from both
		// reclaim phases' range deletes. Partitioned keys must also land in their own keyspace: sharing one
		// with the unpartitioned keys puts them under a prefix scan that reads the partition bytes as a row
		// number, so an unpartitioned scan would sweep every partition's rows.
		let op = build_op(true, None);
		let partition = Partition::of(&[Value::Utf8("sol".to_string())]);

		for (key, expected) in [
			(op.forward_key(RowNumber(42)), KeyspaceId::RINGBUFFER_FORWARD),
			(op.row_entry_key(None, RowNumber(42)), KeyspaceId::RINGBUFFER_ENTRY),
			(op.arm_key(None), KeyspaceId::RINGBUFFER_TTL_ARM),
			(op.meta_key(None), KeyspaceId::RINGBUFFER_META),
			(op.row_entry_key(Some(partition), RowNumber(42)), KeyspaceId::PARTITIONED_RINGBUFFER_ENTRY),
			(op.arm_key(Some(partition)), KeyspaceId::PARTITIONED_RINGBUFFER_TTL_ARM),
			(op.meta_key(Some(partition)), KeyspaceId::PARTITIONED_RINGBUFFER_META),
		] {
			let (group, keyspace, _) = OperatorStateKey::decode_inner(key.as_bytes())
				.expect("a ringbuffer state key must decode as a structured operator-state key");
			assert_eq!(group, GroupId::ROOT, "ringbuffer state must not live inside a reclaimable group");
			assert_eq!(keyspace, expected);
		}
	}

	fn row_entry_count(engine: &TestEngine, op: &SinkRingBufferViewOperator, values: &[Value]) -> usize {
		match partition_of_values(values) {
			None => keyspace_count(engine, op, KeyspaceId::RINGBUFFER_ENTRY, None),
			partition => keyspace_count(engine, op, KeyspaceId::PARTITIONED_RINGBUFFER_ENTRY, partition),
		}
	}

	fn forward_count(engine: &TestEngine, op: &SinkRingBufferViewOperator) -> usize {
		keyspace_count(engine, op, KeyspaceId::RINGBUFFER_FORWARD, None)
	}

	fn metadata(engine: &TestEngine, values: &[Value]) -> Option<RingBufferMetadata> {
		let mut txn = deferred_txn(engine);
		let key = RingBufferMetadataKey::encoded_of(RB, partition_of_values(values));
		txn.get(&[key])
			.unwrap()
			.pop()
			.flatten()
			.map(|row| decode_ringbuffer_metadata(EncodedPodRow::view(&row)).unwrap())
	}

	fn base(value: &str) -> Vec<Value> {
		vec![Value::Utf8(value.to_string())]
	}

	#[test]
	fn a_row_ttl_timer_is_a_noop_when_ttl_disabled() {
		// A ttl-less ring arms no RowTtl timer, so this firing can only arrive by mistake:
		// capacity is its only bound, and evicting here truncates a buffer meant to stay whole.
		let engine = TestEngine::new();
		let mut op = build_op(true, None);
		insert(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1);

		let out = fire(&engine, &mut op, &base("us"), AFTER);
		assert!(out.is_none(), "a ttl-less ring buffer must never evict on a timer");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "no row-entry state may be reclaimed");
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
	}

	#[test]
	fn a_timer_that_fires_before_anything_expires_evicts_nothing() {
		// The direction a cold start lands in: a watermark short of row_time + ttl must find
		// nothing due and never fall back to "evict what looks old".
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));
		insert(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1);

		let out = fire(&engine, &mut op, &base("us"), T0);
		assert!(out.is_none());
		assert_eq!(
			row_entry_count(&engine, &op, &base("us")),
			2,
			"a watermark younger than the horizon may evict nothing"
		);
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
	}

	#[test]
	fn expired_partition_state_is_fully_reclaimed_and_active_partition_survives() {
		// A quiet partition's whole per-partition state must be reclaimed rather than stranded,
		// while a partition that received fresher rows is left untouched.
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &mut op, true, &[("eu", 3), ("eu", 4)], 3, AFTER);

		let out = fire(&engine, &mut op, &base("us"), AFTER);
		assert!(
			out.is_some(),
			"an eviction that reclaims real rows must still advance the operator's watermark"
		);

		assert!(
			metadata(&engine, &base("us")).is_none(),
			"the fully expired partition must lose its metadata key"
		);
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 0, "its row entries must be gone");

		let eu = metadata(&engine, &base("eu")).expect("the fresh partition keeps its metadata");
		assert_eq!(eu.count, 2, "the fresh partition must be untouched");
		assert_eq!(row_entry_count(&engine, &op, &base("eu")), 2);
		assert_eq!(forward_count(&engine, &op), 2, "only the two surviving eu forward mappings remain");
	}

	#[test]
	fn ttl_expiry_reclaims_state_but_never_announces_downstream() {
		// Unlike capacity eviction, ttl expiry must never be visible to downstream views.
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &mut op, true, &[("eu", 3), ("eu", 4)], 3, AFTER);

		let mut txn = deferred_txn(&engine);
		let key = op.timer_key(partition_of_values(&base("us")));
		let out = op
			.on_timer(
				&mut txn,
				Timer {
					due: DateTime::from_nanos(AFTER),
					kind: TimerKind::RowTtl,
					key,
				},
			)
			.unwrap();
		let row_writes: Vec<PendingWrite> = txn
			.pending
			.rows_ordered()
			.filter(|(key, _)| {
				matches!(
					TaggedKey::decode(key),
					Some(TaggedKey::Row(RowKey {
						storage: RB,
						..
					})) | Some(TaggedKey::PartitionedRow(PartitionedRowKey {
						storage: RB,
						..
					}))
				)
			})
			.map(|(_, write)| write.clone())
			.collect();
		let emitted = txn.emitted;
		commit_flow_pending(&engine, &mut txn);

		assert!(out.is_some(), "reclaiming real rows must still advance the operator's watermark");
		assert!(!emitted, "ttl expiry must never mark the transaction as having emitted output");
		assert!(!row_writes.is_empty(), "ttl expiry must remove the expired rows");
		assert!(
			row_writes.iter().all(|write| matches!(
				write,
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved
				}
			)),
			"ttl expiry must remove rows unobserved so no downstream view sees them: {row_writes:?}"
		);
		assert!(metadata(&engine, &base("us")).is_none(), "the expired partition's state is still reclaimed");
	}

	#[test]
	fn partial_expiry_decrements_count_and_keeps_the_write_cycle() {
		// Rewinding the cycle on expiry would make the next insert overwrite a live row instead of the oldest
		// slot.
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &mut op, true, &[("us", 3), ("us", 4)], 3, AFTER);

		let before = metadata(&engine, &base("us")).unwrap();
		assert_eq!(before.count, 4);

		fire(&engine, &mut op, &base("us"), AFTER);

		let after = metadata(&engine, &base("us")).expect("partition still has survivors");
		assert_eq!(after.count, 2, "the two expired rows must be subtracted");
		assert_eq!(after.base_rn, before.base_rn, "expiry must not move the partition's slot range");
		assert_eq!(after.next_sequence, before.next_sequence, "expiry must not rewind the write cycle");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "only the two fresh row entries remain");
	}

	#[test]
	fn non_partitioned_eviction_reclaims_state() {
		let engine = TestEngine::new();
		let mut op = build_op(false, Some(hour_ttl()));

		insert_at(&engine, &mut op, false, &[("", 1), ("", 2)], 1, T0);
		insert_at(&engine, &mut op, false, &[("", 3)], 3, AFTER);

		fire(&engine, &mut op, &[], AFTER);

		let global = metadata(&engine, &[]).expect("the global ring keeps a single metadata key");
		assert_eq!(global.count, 1, "only the fresh row remains counted");
		assert_eq!(row_entry_count(&engine, &op, &[]), 1, "only the fresh row entry remains");
		assert_eq!(forward_count(&engine, &op), 1);
	}

	#[test]
	fn an_expired_row_behind_a_live_head_waits_until_the_head_expires() {
		// Expiry walks from the oldest slot and stops at the first live row, so an out-of-order old stamp
		// stays.
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1)], 1, AFTER);
		insert_at(&engine, &mut op, true, &[("us", 2)], 2, T0);

		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2, "precondition: both rows are live");

		let out = fire(&engine, &mut op, &base("us"), AFTER);

		assert!(out.is_none(), "the head row is live, so nothing expires");
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2);
	}

	fn build_capped(partitioned: bool, capacity: u64) -> SinkRingBufferViewOperator {
		let mut op = build_op(partitioned, Some(hour_ttl()));
		op.capacity = capacity;
		op
	}

	fn rows_of(partitioned: bool, per_partition: usize, first_n: i32) -> Vec<(&'static str, i32)> {
		let bases: &[&'static str] = if partitioned {
			&["us", "eu"]
		} else {
			&[""]
		};
		(0..per_partition as i32).flat_map(|i| bases.iter().map(move |base| (*base, first_n + i))).collect()
	}

	fn counted(engine: &TestEngine, op: &mut SinkRingBufferViewOperator, diff: Diff) -> StorageCounts {
		let mut txn = deferred_txn(engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				OperatorId(1),
				ChangeVersion::from(CommitVersion(1)),
				vec![diff],
				DateTime::from_nanos(T0),
			),
		)
		.unwrap();
		let counts = txn.storage_counts;
		commit_flow_pending(engine, &mut txn);
		counts
	}

	fn eviction_counts(partitioned: bool, evicted: usize) -> StorageCounts {
		let engine = TestEngine::new();
		let mut op = build_capped(partitioned, 3);
		let stored = rows_of(partitioned, 3, 1);
		insert(&engine, &mut op, partitioned, &stored, 1);
		let incoming = rows_of(partitioned, evicted, 100);
		let first_rn = stored.len() as u64 + 1;
		counted(&engine, &mut op, Diff::insert(columns_at(partitioned, &incoming, first_rn, T0)))
	}

	#[test]
	fn a_capacity_eviction_reads_storage_as_often_for_three_evicted_rows_as_for_one() {
		// A read per evicted row turns every insert into a full buffer into one storage round trip per row.
		for partitioned in [false, true] {
			let (more, fewer) = (eviction_counts(partitioned, 3), eviction_counts(partitioned, 1));
			assert!(
				reads_no_more(&more, &fewer),
				"evicting more rows must not add storage reads (partitioned: {partitioned}): {more:?} vs {fewer:?}"
			);
		}
	}

	fn reads_no_more(more: &StorageCounts, fewer: &StorageCounts) -> bool {
		// The ttl timer reads the head slot once unless this apply already holds it, so fewer rows may read one
		// more.
		more.gets <= fewer.gets
			&& more.get_many_calls <= fewer.get_many_calls
			&& more.state_fetch_calls <= fewer.state_fetch_calls
			&& more.ranges <= fewer.ranges
	}

	fn stored_three(partitioned: bool) -> (TestEngine, SinkRingBufferViewOperator, Vec<(&'static str, i32)>) {
		let engine = TestEngine::new();
		let mut op = build_op(partitioned, Some(hour_ttl()));
		let stored = rows_of(partitioned, 3, 1);
		insert(&engine, &mut op, partitioned, &stored, 1);
		(engine, op, stored)
	}

	fn remove_counts(partitioned: bool, removed: usize) -> StorageCounts {
		let (engine, mut op, stored) = stored_three(partitioned);
		let width = if partitioned {
			2
		} else {
			1
		};
		let pre = columns_at(partitioned, &stored[..removed * width], 1, T0);
		counted(&engine, &mut op, Diff::remove(pre))
	}

	#[test]
	fn a_remove_reads_storage_as_often_for_three_rows_as_for_one() {
		// A forward and row-entry read per removed row makes a bulk delete cost one round trip per row.
		for partitioned in [false, true] {
			let (more, fewer) = (remove_counts(partitioned, 3), remove_counts(partitioned, 1));
			assert!(
				reads_no_more(&more, &fewer),
				"removing more rows must not add storage reads (partitioned: {partitioned}): {more:?} vs {fewer:?}"
			);
		}
	}

	fn update_counts(partitioned: bool, updated: usize) -> StorageCounts {
		let (engine, mut op, stored) = stored_three(partitioned);
		let width = if partitioned {
			2
		} else {
			1
		};
		let pre_rows = &stored[..updated * width];
		let post_rows: Vec<(&str, i32)> = pre_rows.iter().map(|(base, n)| (*base, n + 1000)).collect();
		let pre = columns_at(partitioned, pre_rows, 1, T0);
		let post = columns_at(partitioned, &post_rows, 1, T0);
		counted(&engine, &mut op, Diff::update(pre, post))
	}

	#[test]
	fn an_update_reads_storage_as_often_for_three_rows_as_for_one() {
		// A forward read per updated row makes a bulk update cost one round trip per row.
		for partitioned in [false, true] {
			assert_eq!(
				update_counts(partitioned, 3),
				update_counts(partitioned, 1),
				"updating more rows must not add storage reads (partitioned: {partitioned})"
			);
		}
	}

	fn expiry_counts(partitioned: bool, expired: usize) -> StorageCounts {
		let engine = TestEngine::new();
		let mut op = build_op(partitioned, Some(hour_ttl()));
		insert_at(&engine, &mut op, partitioned, &rows_of(partitioned, expired, 1), 1, T0);
		let mut txn = deferred_txn(&engine);
		let partition = if partitioned {
			base("us")
		} else {
			Vec::new()
		};
		let key = op.timer_key(partition_of_values(&partition));
		op.on_timer(
			&mut txn,
			Timer {
				due: DateTime::from_nanos(AFTER),
				kind: TimerKind::RowTtl,
				key,
			},
		)
		.unwrap()
		.expect("the due rows are evicted");
		txn.storage_counts
	}

	#[test]
	fn a_ttl_eviction_reads_storage_as_often_for_three_due_rows_as_for_one() {
		// A row-entry read per due row makes every expiry sweep one round trip per expired row.
		for partitioned in [false, true] {
			assert_eq!(
				expiry_counts(partitioned, 3),
				expiry_counts(partitioned, 1),
				"expiring more rows must not add storage reads (partitioned: {partitioned})"
			);
		}
	}

	#[test]
	fn an_apply_that_leaves_the_metadata_unchanged_writes_no_metadata() {
		// Rewriting unchanged metadata on every apply makes a step with nothing to store still commit.
		let (engine, mut op, stored) = stored_three(false);
		let post_rows: Vec<(&str, i32)> = stored.iter().map(|(base, n)| (*base, n + 1000)).collect();
		let mut txn = deferred_txn(&engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				OperatorId(1),
				ChangeVersion::from(CommitVersion(1)),
				vec![Diff::update(
					columns_at(false, &stored, 1, T0),
					columns_at(false, &post_rows, 1, T0),
				)],
				DateTime::from_nanos(T0),
			),
		)
		.unwrap();

		assert!(
			txn.pending.write_at(&RingBufferMetadataKey::encoded(RB)).is_none(),
			"an update moves no ring position, so the metadata row must not be rewritten"
		);
		assert!(
			txn.pending.write_at(&scoped_key(OperatorId(1), &op.meta_key(None))).is_none(),
			"the metadata mirror must not be rewritten either"
		);
		assert!(txn.emitted, "the update itself still reaches the view");
	}

	fn single_rows(rows: &[(i32, u64)]) -> RecordBatch {
		let ts: Vec<DateTime> = rows.iter().map(|_| DateTime::from_nanos(T0)).collect();
		let stamp = || -> ArrayRef { Arc::new(datetime_array(ts.clone())) };
		let rns: Vec<u64> = rows.iter().map(|(_, rn)| *rn).collect();
		let system: [(SystemColumn, ArrayRef); 4] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(rns))),
			(SystemColumn::CreatedAt, stamp()),
			(SystemColumn::UpdatedAt, stamp()),
			(SystemColumn::Time, stamp()),
		];
		let columns = batch(vec![int4("n", rows.iter().map(|(n, _)| *n).collect::<Vec<i32>>())]).unwrap();
		system.into_iter()
			.fold(columns, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	#[test]
	fn a_remove_naming_a_row_twice_frees_its_slot_once() {
		// A forward read once for the whole diff still finds a row the diff already removed, so a second
		// copy would free a second slot.
		let (engine, mut op, _) = stored_three(false);
		counted(&engine, &mut op, Diff::remove(single_rows(&[(1, 1), (1, 1)])));

		assert_eq!(metadata(&engine, &[]).unwrap().count, 2, "the row is removed once");
		assert_eq!(forward_count(&engine, &op), 2);
	}

	#[test]
	fn a_source_row_inserted_again_while_its_old_copy_is_evicted_keeps_its_new_forward() {
		// The evicted copy's forward removal must land before the new copy's forward write, otherwise the
		// new row is stored with no forward and a later remove misses it.
		let engine = TestEngine::new();
		let mut op = build_capped(false, 1);
		counted(&engine, &mut op, Diff::insert(single_rows(&[(10, 1)])));
		counted(&engine, &mut op, Diff::insert(single_rows(&[(20, 1)])));
		assert_eq!(forward_count(&engine, &op), 1, "the source row keeps one forward");

		counted(&engine, &mut op, Diff::remove(single_rows(&[(20, 1)])));

		assert_eq!(metadata(&engine, &[]).unwrap().count, 0, "the remove finds the new copy");
		assert_eq!(row_entry_count(&engine, &op, &[]), 0);
	}

	fn build_capped_op(capacity: u64) -> SinkRingBufferViewOperator {
		let View::RingBuffer(mut def) = view_def(false) else {
			unreachable!()
		};
		def.capacity = capacity;
		let resolved = ResolvedView::new(
			Fragment::internal("rb"),
			ResolvedNamespace::new(Fragment::internal("test"), Namespace::system()),
			View::RingBuffer(def),
		);
		SinkRingBufferViewOperator::new(
			OperatorId(1),
			resolved,
			capacity,
			None,
			Vec::new(),
			RuntimeContext::with_clock(Clock::Mock(MockClock::from_millis(0))),
		)
	}

	fn row_writes(txn: &DeferredTransaction) -> Vec<(TaggedKey, PendingWrite)> {
		txn.pending
			.rows_ordered()
			.filter_map(|(key, write)| match TaggedKey::decode(key) {
				Some(
					key @ TaggedKey::Row(RowKey {
						storage: RB,
						..
					}),
				) => Some((key, write.clone())),
				_ => None,
			})
			.collect()
	}

	fn push_out(capacity: u64, stored: &[(&'static str, i32)]) -> DeferredTransaction {
		let engine = TestEngine::new();
		let mut op = build_capped_op(capacity);
		insert(&engine, &mut op, false, stored, 1);

		let mut txn = deferred_txn(&engine);
		txn.capture_emits();
		op.apply(
			&mut txn,
			Change::from_flow(
				OperatorId(1),
				ChangeVersion::from(CommitVersion(1)),
				vec![Diff::insert(columns_at(false, &[("", 9)], stored.len() as u64 + 1, T0))],
				DateTime::from_nanos(T0),
			),
		)
		.unwrap();
		txn
	}

	#[test]
	fn a_capacity_push_out_overwrites_the_oldest_slot_in_place() {
		// A remove plus a set on a fresh key is the remove + insert pair downstream joins paid for four times.
		let txn = push_out(1, &[("", 1)]);
		let writes = row_writes(&txn);
		assert_eq!(writes.len(), 1, "a push-out writes exactly one row key: {writes:?}");
		assert!(matches!(writes[0].1, PendingWrite::Set(_)), "the write is a set, not a remove: {writes:?}");
		assert_eq!(
			writes[0].0,
			TaggedKey::Row(RowKey {
				storage: RB,
				row: RowNumber(1),
			}),
			"the new row keeps the evicted row's slot"
		);
	}

	#[test]
	fn a_capacity_push_out_announces_one_update_whose_pre_is_the_oldest_row() {
		// Evicting the newest slot instead of the oldest would retract a row readers still expect to see.
		let mut txn = push_out(3, &[("", 1), ("", 2), ("", 3)]);
		let emitted = txn.take_emits(ViewId(1));
		assert_eq!(emitted.len(), 1, "one diff per push-out: {emitted:?}");
		let Diff::Update {
			pre,
			post,
			..
		} = &emitted[0]
		else {
			panic!("a push-out must announce an update, got {emitted:?}");
		};
		assert_eq!(cell(pre, "n"), Value::Int4(1), "the pre is the oldest row");
		assert_eq!(cell(post, "n"), Value::Int4(9));
	}

	fn cell(columns: &RecordBatch, name: &str) -> Value {
		let index = columns.schema_ref().index_of(name).unwrap();
		ColumnView::try_from((columns.column(index), columns.schema_ref().field(index))).unwrap().get_value(0)
	}

	#[test]
	fn a_late_remove_of_an_evicted_source_row_leaves_the_new_occupant() {
		// A forward kept across the overwrite would let the old source row's remove delete the row now in its
		// slot.
		let engine = TestEngine::new();
		let mut op = build_capped_op(1);
		counted(&engine, &mut op, Diff::insert(single_rows(&[(10, 1)])));
		counted(&engine, &mut op, Diff::insert(single_rows(&[(20, 2)])));

		counted(&engine, &mut op, Diff::remove(single_rows(&[(10, 1)])));

		assert_eq!(metadata(&engine, &[]).unwrap().count, 1, "the new occupant survives");
		assert_eq!(forward_count(&engine, &op), 1, "only the occupant's forward remains");
		let mut txn = deferred_txn(&engine);
		let stored = txn.get(&[RowKey::encoded(RB, RowNumber(1))]).unwrap().pop().flatten();
		assert!(stored.is_some(), "the slot still holds the new row");
	}

	fn row_removes_of_a_ttl_expiry(ttl_eviction: EvictionVisibility) -> Vec<PendingWrite> {
		let engine = TestEngine::new();
		let mut op = build_op(false, Some(hour_ttl())).with_ttl_eviction(ttl_eviction);
		insert_at(&engine, &mut op, false, &[("us", 1), ("us", 2)], 1, T0);

		let mut txn = deferred_txn(&engine);
		let key = op.timer_key(None);
		op.on_timer(
			&mut txn,
			Timer {
				due: DateTime::from_nanos(AFTER),
				kind: TimerKind::RowTtl,
				key,
			},
		)
		.unwrap();
		txn.pending
			.rows_ordered()
			.filter(|(key, write)| {
				matches!(
					TaggedKey::decode(key),
					Some(TaggedKey::Row(RowKey {
						storage: RB,
						..
					}))
				) && matches!(write, PendingWrite::Remove { .. })
			})
			.map(|(_, write)| write.clone())
			.collect()
	}

	#[test]
	fn a_visible_ttl_expiry_is_announced() {
		// An unobserved remove here would leave every expired row in the downstream views of a ttl visible
		// ring.
		let removes = row_removes_of_a_ttl_expiry(EvictionVisibility::Visible);
		assert_eq!(removes.len(), 2, "both expired rows must be removed: {removes:?}");
		assert!(
			removes.iter().all(|write| matches!(
				write,
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced
				}
			)),
			"a visible ttl expiry must announce every remove: {removes:?}"
		);
	}

	#[test]
	fn a_silent_ttl_expiry_stays_unobserved() {
		// The explicit silent policy must keep today's unobserved expiry, never announce it.
		let removes = row_removes_of_a_ttl_expiry(EvictionVisibility::Silent);
		assert_eq!(removes.len(), 2, "both expired rows must be removed: {removes:?}");
		assert!(
			removes.iter().all(|write| matches!(
				write,
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved
				}
			)),
			"a silent ttl expiry must keep every remove unobserved: {removes:?}"
		);
	}
}
