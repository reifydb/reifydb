// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
};

use arrow_array::RecordBatch;
use indexmap::IndexSet;
use reifydb_codec::{
	key::{
		encode_i64_asc, encode_u64_asc, encode_u128_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	row::{
		bytes::EncodedBytes,
		operator::state::{decode, encode},
		pod::EncodedPodRow,
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	common::ChangeVersion,
	interface::{
		catalog::{
			flow::OperatorId,
			ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata, encode_ringbuffer_metadata},
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
				PartitionedRingbufferEntry, PartitionedRingbufferEntryKey, PartitionedRingbufferExpiry,
				PartitionedRingbufferExpiryKey, PartitionedRingbufferMeta,
				PartitionedRingbufferMetaKey, PartitionedRingbufferTtlArm,
				PartitionedRingbufferTtlArmKey, RingbufferEntry, RingbufferEntryKey, RingbufferExpiry,
				RingbufferExpiryKey, RingbufferMeta, RingbufferMetaKey, RingbufferTtlArm,
				RingbufferTtlArmKey,
			},
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, node_prefix},
		},
		ringbuffer::RingBufferMetadataKey,
		row::{PartitionedRowKey, RowKey},
		typed::direction::Asc,
	},
	partition::partition_col_indices,
	row::row_shape_from_columns,
	state::{
		timer::TimerKind,
		typed::{SuffixBytes, typed_key},
	},
};
use reifydb_flow::{
	operator::sink::{
		SourceRowEncoder, coerce_columns,
		partition::{ensure_partition_unchanged, partition_batch, resolve_partitions_flow},
		shape_field_columns, stored_created_at_many,
		table::{
			dictionary_encode_view_columns, dictionary_lookup_partition_columns,
			dictionary_lookup_view_columns,
		},
	},
	txn::{Batch, Emit, Get},
};
use reifydb_macro::operator_state;
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::multi::RangeScope;
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
	operator::{join::column::JoinedColumnsBuilder, row_times, state::iter::StateIterator},
	timer::{Timer, extension::TimerExtension},
	transaction::{FlowTransaction, deferred::DeferredTransaction, state::StateExtension},
};

fn row_entry_prefix(partition: Option<Partition>) -> Vec<u8> {
	match partition {
		Some(partition) => {
			let mut prefix = OperatorStateKey::inner_encoded(
				GroupId::ROOT,
				KeyspaceId::PARTITIONED_RINGBUFFER_ENTRY,
				[],
			)
			.as_slice()
			.to_vec();
			prefix.extend_from_slice(&encode_u128_asc(partition.0));
			prefix
		}
		None => OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::RINGBUFFER_ENTRY, [])
			.as_slice()
			.to_vec(),
	}
}

fn expiry_scan_prefix(partition: Option<Partition>) -> Vec<u8> {
	match partition {
		Some(partition) => {
			let mut prefix = OperatorStateKey::inner_encoded(
				GroupId::ROOT,
				KeyspaceId::PARTITIONED_RINGBUFFER_EXPIRY,
				[],
			)
			.as_slice()
			.to_vec();
			prefix.extend_from_slice(&encode_u128_asc(partition.0));
			prefix
		}
		None => OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::RINGBUFFER_EXPIRY, [])
			.as_slice()
			.to_vec(),
	}
}

fn note_touched(touched: &mut IndexSet<Vec<Value>>, partition_values: &[Value]) {
	if !touched.contains(partition_values) {
		touched.insert(partition_values.to_vec());
	}
}

#[allow(clippy::disallowed_methods)]
fn partition_of_values(partition_values: &[Value]) -> Option<Partition> {
	(!partition_values.is_empty()).then(|| Partition::of(partition_values))
}

fn decode_expiry_key(partition: Option<Partition>, key: &EncodedKey) -> Result<(i64, u64)> {
	let suffix = OperatorStateKey::decode_inner(key.as_slice()).map(|(_, _, suffix)| suffix.to_vec()).ok_or_else(
		|| {
			Error::from(FlowStateError::Decode {
				state: "RingBufferExpiry",
				cause: "expiry key is not a framed operator-state key".to_string(),
			})
		},
	)?;
	let decoded = match partition {
		Some(_) => PartitionedRingbufferExpiryKey::from_suffix_bytes(&suffix)
			.map(|key| (key.expires_at.0, key.row.0.0)),
		None => RingbufferExpiryKey::from_suffix_bytes(&suffix).map(|key| (key.expires_at.0, key.row.0.0)),
	};
	decoded.ok_or_else(|| {
		Error::from(FlowStateError::Decode {
			state: "RingBufferExpiry",
			cause: "expiry key does not match its keyspace layout".to_string(),
		})
	})
}

fn decode_row_entry_key(partition: Option<Partition>, key: &EncodedKey) -> Result<u64> {
	let suffix = OperatorStateKey::decode_inner(key.as_slice()).map(|(_, _, suffix)| suffix.to_vec()).ok_or_else(
		|| {
			Error::from(FlowStateError::Decode {
				state: "RingBufferRowEntry",
				cause: "row-entry key is not a framed operator-state key".to_string(),
			})
		},
	)?;
	let decoded = match partition {
		Some(_) => PartitionedRingbufferEntryKey::from_suffix_bytes(&suffix).map(|key| key.row.0.0),
		None => RingbufferEntryKey::from_suffix_bytes(&suffix).map(|key| key.row.0.0),
	};
	decoded.ok_or_else(|| {
		Error::from(FlowStateError::Decode {
			state: "RingBufferRowEntry",
			cause: "row-entry key does not match its keyspace layout".to_string(),
		})
	})
}

type PartitionMetadata = HashMap<Vec<Value>, (RingBufferMetadata, RingBufferMetadata)>;

pub struct SinkRingBufferViewOperator {
	operator: OperatorId,
	view: ResolvedView,
	shape: RowShape,
	storage: StorageId,
	capacity: u64,
	ttl: Option<Duration>,
	partition_indices: Vec<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
	runtime_context: RuntimeContext,
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
		Self {
			operator,
			view,
			shape,
			storage,
			capacity,
			ttl,
			partition_indices,
			verified_partitions: HashMap::new(),
			runtime_context,
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	#[inline]
	fn rb_key(&self, object_id: StorageId, rn: RowNumber, partition: Option<Partition>) -> EncodedKey {
		match partition {
			Some(partition) => PartitionedRowKey::encoded(object_id, partition, rn),
			None => RowKey::encoded(object_id, rn),
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
		let Some(row) = self.state_get(txn, &key)? else {
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
		self.state_set(txn, &key, row)
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
			let stored = txn.get(mvcc)?.map(|row| decode_ringbuffer_metadata(EncodedPodRow::view(&row))).transpose()?;
			assert_eq!(
				mirrored, stored,
				"the ringbuffer metadata mirror and its mvcc row disagree right after a write; the \
				 mirror is what replay reads, so any drift hands catch-up a different tail than the \
				 live run assigned and the operator state forward map diverges silently"
			);
		}
		Ok(())
	}

	fn read_metadata(&self, txn: &mut DeferredTransaction) -> Result<RingBufferMetadata> {
		if let Some(metadata) = self.read_meta_mirror(txn, None)? {
			return Ok(metadata);
		}
		let key = RingBufferMetadataKey::encoded(self.storage);
		let metadata = match txn.get(&key)? {
			Some(row) => decode_ringbuffer_metadata(EncodedPodRow::view(&row))?,
			None => RingBufferMetadata::new(),
		};
		self.write_meta_mirror(txn, None, &metadata)?;
		Ok(metadata)
	}

	fn write_metadata(&self, txn: &mut DeferredTransaction, metadata: &RingBufferMetadata) -> Result<()> {
		let key = RingBufferMetadataKey::encoded(self.storage);
		let row = encode_ringbuffer_metadata(metadata);
		txn.set_one(&key, row.into_bytes())?;
		self.write_meta_mirror(txn, None, metadata)?;
		self.assert_mirrors_mvcc(txn, None, &key)
	}

	fn read_partition_metadata(
		&self,
		txn: &mut DeferredTransaction,
		partition_values: &[Value],
	) -> Result<RingBufferMetadata> {
		let partition = partition_of_values(partition_values);
		if let Some(metadata) = self.read_meta_mirror(txn, partition)? {
			return Ok(metadata);
		}
		let key = RingBufferMetadataKey::encoded_partition(self.storage, partition_values.to_vec());
		let metadata = match txn.get(&key)? {
			Some(row) => decode_ringbuffer_metadata(EncodedPodRow::view(&row))?,
			None => RingBufferMetadata::new(),
		};
		self.write_meta_mirror(txn, partition, &metadata)?;
		Ok(metadata)
	}

	fn write_partition_metadata(
		&self,
		txn: &mut DeferredTransaction,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.storage, partition_values.to_vec());
		let row = encode_ringbuffer_metadata(metadata);
		txn.set_one(&key, row.into_bytes())?;
		let partition = partition_of_values(partition_values);
		self.write_meta_mirror(txn, partition, metadata)?;
		self.assert_mirrors_mvcc(txn, partition, &key)
	}

	fn remove_partition_metadata(&self, txn: &mut DeferredTransaction, partition_values: &[Value]) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.storage, partition_values.to_vec());
		txn.remove_one(&key)?;
		let partition = partition_of_values(partition_values);
		self.state_remove(txn, &self.meta_key(partition))?;
		self.assert_mirrors_mvcc(txn, partition, &key)
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
		self.state_get_many(txn, &keys)?
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

	fn expiry_key(&self, partition: Option<Partition>, expires_at: i64, storage_rn: RowNumber) -> GroupStateKey {
		match partition {
			Some(partition) => typed_key::<PartitionedRingbufferExpiry>(
				GroupId::ROOT,
				&PartitionedRingbufferExpiryKey {
					partition: Asc(partition),
					expires_at: Asc(expires_at),
					row: Asc(storage_rn),
				},
			),
			None => typed_key::<RingbufferExpiry>(
				GroupId::ROOT,
				&RingbufferExpiryKey {
					expires_at: Asc(expires_at),
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

	fn row_entries(
		&self,
		txn: &mut DeferredTransaction,
		slots: &[(Option<Partition>, RowNumber)],
	) -> Result<Vec<Option<(Option<DateTime>, RowNumber)>>> {
		let keys: Vec<GroupStateKey> =
			slots.iter().map(|(partition, rn)| self.row_entry_key(*partition, *rn)).collect();
		self.state_get_many(txn, &keys)?
			.into_iter()
			.map(|row| row.map(|row| self.decode_row_entry(&row)).transpose())
			.collect()
	}

	fn push_row_entry_sets(
		&self,
		sets: &mut Vec<(GroupStateKey, EncodedPodRow)>,
		partition: Option<Partition>,
		storage_rn: RowNumber,
		source_rn: RowNumber,
		time: Option<DateTime>,
	) -> Result<()> {
		sets.push((self.forward_key(source_rn), encode_u64(storage_rn.0, "RingBufferForward")?));
		let entry = RowEntry {
			source_rn,
			time,
		};
		let row = encode(&entry).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "RingBufferRowEntry",
				cause: e.to_string(),
			})
		})?;
		sets.push((self.row_entry_key(partition, storage_rn), row));
		if let Some(expires_at) = self.expires_at(time) {
			sets.push((self.expiry_key(partition, expires_at, storage_rn), EncodedPodRow::new(&[])));
		}
		Ok(())
	}

	fn push_row_entry_removes(
		&self,
		removes: &mut Vec<GroupStateKey>,
		partition: Option<Partition>,
		storage_rn: RowNumber,
		time: Option<DateTime>,
	) {
		if let Some(expires_at) = self.expires_at(time) {
			removes.push(self.expiry_key(partition, expires_at, storage_rn));
		}
		removes.push(self.row_entry_key(partition, storage_rn));
	}

	fn decode_row_entry(&self, row: &EncodedPodRow) -> Result<(Option<DateTime>, RowNumber)> {
		let entry: RowEntry = decode(row).map_err(|e| {
			Error::from(FlowStateError::Decode {
				state: "RingBufferRowEntry",
				cause: e.to_string(),
			})
		})?;
		Ok((entry.time, entry.source_rn))
	}
}

#[operator_state]
struct RowEntry {
	source_rn: RowNumber,
	time: Option<DateTime>,
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

impl SinkRingBufferViewOperator {
	fn state_get(&self, txn: &mut DeferredTransaction, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		txn.state_get(self.operator, key)
	}

	fn state_get_many(
		&self,
		txn: &mut DeferredTransaction,
		keys: &[GroupStateKey],
	) -> Result<Vec<Option<EncodedPodRow>>> {
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		txn.state_get_many(self.operator, keys)
	}

	fn state_set(&self, txn: &mut DeferredTransaction, key: &GroupStateKey, row: EncodedPodRow) -> Result<()> {
		txn.state_set(self.operator, key, row)
	}

	fn state_set_many(
		&self,
		txn: &mut DeferredTransaction,
		rows: Vec<(GroupStateKey, EncodedPodRow)>,
	) -> Result<()> {
		if rows.is_empty() {
			return Ok(());
		}
		txn.state_set_many(self.operator, rows)
	}

	fn state_remove_many(&self, txn: &mut DeferredTransaction, keys: &[GroupStateKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}
		txn.state_remove_many(self.operator, keys)
	}

	fn state_remove(&self, txn: &mut DeferredTransaction, key: &GroupStateKey) -> Result<()> {
		txn.state_remove(self.operator, key)
	}

	fn state_range<'a>(&self, txn: &'a mut DeferredTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
		let prefixed = range.with_prefix(EncodedKey::new(node_prefix(self.operator)));
		StateIterator::new(txn.range(prefixed, RangeScope::All, 1024))
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
		let resolved = self.view.clone();
		let view = resolved.def();
		let shape = self.shape.clone();
		let object_id = self.storage;
		let loaded = if self.is_partitioned() {
			None
		} else {
			Some(self.read_metadata(txn)?)
		};
		let mut metadata = loaded.clone();
		let mut partition_metadata: PartitionMetadata = HashMap::new();
		let mut touched: IndexSet<Vec<Value>> = IndexSet::new();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_ringbuffer_insert(
					txn,
					view,
					&shape,
					object_id,
					&mut metadata,
					&mut partition_metadata,
					post,
					&mut touched,
				)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_ringbuffer_update(
					txn,
					view,
					&shape,
					object_id,
					pre,
					post,
					&mut touched,
				)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_ringbuffer_remove(
					txn,
					view,
					object_id,
					&mut metadata,
					&mut partition_metadata,
					pre,
					&mut touched,
				)?,
			}
		}

		if let Some(metadata) = metadata.as_ref().filter(|_| metadata != loaded) {
			self.write_metadata(txn, metadata)?;
		}
		for (partition_values, (before, partition_meta)) in partition_metadata.iter() {
			if partition_meta == before {
				continue;
			}
			if partition_meta.is_empty() {
				self.remove_partition_metadata(txn, partition_values)?;
			} else {
				self.write_partition_metadata(txn, partition_values, partition_meta)?;
			}
		}

		for partition_values in &touched {
			self.sync_row_ttl_timer(txn, partition_values)?;
		}

		Ok(Change::from_flow(self.operator, change.version, Vec::new(), change.changed_at))
	}

	fn on_timer(&mut self, txn: &mut DeferredTransaction, timer: Timer) -> Result<Option<Change>> {
		if timer.kind != TimerKind::RowTtl || self.ttl.is_none() {
			return Ok(None);
		}
		let partition_values = self.timer_partition_values(&timer.key)?;

		let object_id = self.storage;
		let evicted_any = self.evict_due(txn, object_id, &partition_values, timer.due.to_millis())?;
		self.sync_row_ttl_timer(txn, &partition_values)?;

		if evicted_any {
			let version = ChangeVersion::from(txn.version());
			return Ok(Some(Change::from_flow(self.operator, version, Vec::new(), timer.due)));
		}
		Ok(None)
	}
}

impl SinkRingBufferViewOperator {
	fn due_storage_rows(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
		at: i64,
	) -> Result<Vec<u64>> {
		let prefix = expiry_scan_prefix(partition);
		let mut end = prefix.clone();
		end.extend_from_slice(&encode_i64_asc(at.saturating_add(1)));
		let range = EncodedKeyRange::new(
			Bound::Included(EncodedKey::new(prefix)),
			Bound::Excluded(EncodedKey::new(end)),
		);
		self.state_range(txn, range)
			.map(|result| {
				let (key, _row) = result?;
				Ok(decode_expiry_key(partition, &key)?.1)
			})
			.collect()
	}

	fn lowest_storage_row(
		&self,
		txn: &mut DeferredTransaction,
		partition: Option<Partition>,
	) -> Result<Option<u64>> {
		let range = EncodedKeyRange::prefix(&row_entry_prefix(partition));
		match self.state_range(txn, range).next() {
			Some(result) => {
				let (key, _row) = result?;
				Ok(Some(decode_row_entry_key(partition, &key)?))
			}
			None => Ok(None),
		}
	}

	fn earliest_expiry(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<Option<i64>> {
		let range = EncodedKeyRange::prefix(&expiry_scan_prefix(partition));
		match self.state_range(txn, range).next() {
			Some(result) => {
				let (key, _row) = result?;
				Ok(Some(decode_expiry_key(partition, &key)?.0))
			}
			None => Ok(None),
		}
	}

	fn timer_key(&self, partition_values: &[Value]) -> EncodedKey {
		match partition_values.is_empty() {
			true => RingBufferMetadataKey::encoded(self.storage),
			false => RingBufferMetadataKey::encoded_partition(self.storage, partition_values.to_vec()),
		}
	}

	fn timer_partition_values(&self, key: &EncodedKey) -> Result<Vec<Value>> {
		RingBufferMetadataKey::decode(key).map(|decoded| decoded.partition_values).ok_or_else(|| {
			Error::from(FlowStateError::Decode {
				state: "RingBufferMetadataKey",
				cause: "a RowTtl timer key must decode as ring buffer metadata".to_string(),
			})
		})
	}

	fn read_armed(&self, txn: &mut DeferredTransaction, partition: Option<Partition>) -> Result<Option<i64>> {
		match self.state_get(txn, &self.arm_key(partition))? {
			Some(row) => Ok(Some(decode_i64(&row, "RingBufferTtlArm")?)),
			None => Ok(None),
		}
	}

	fn sync_row_ttl_timer(&self, txn: &mut DeferredTransaction, partition_values: &[Value]) -> Result<()> {
		if self.ttl.is_none() {
			return Ok(());
		}
		let partition = partition_of_values(partition_values);
		let armed = self.read_armed(txn, partition)?;
		let earliest = self.earliest_expiry(txn, partition)?;
		if armed == earliest {
			return Ok(());
		}
		let key = self.timer_key(partition_values);
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
				self.state_set(txn, &arm_key, encode_i64(at, "RingBufferTtlArm")?)
			}
			None => self.state_remove(txn, &arm_key),
		}
	}

	fn evict_due(
		&self,
		txn: &mut DeferredTransaction,
		object_id: StorageId,
		partition_values: &[Value],
		at: i64,
	) -> Result<bool> {
		let partition = partition_of_values(partition_values);

		let to_evict = self.due_storage_rows(txn, partition, at)?;
		if to_evict.is_empty() {
			return Ok(false);
		}

		let evicted_count = to_evict.len() as u64;
		let slots: Vec<(Option<Partition>, RowNumber)> =
			to_evict.into_iter().map(|storage_rn| (partition, RowNumber(storage_rn))).collect();
		let entries = self.row_entries(txn, &slots)?;
		let mut removes: Vec<GroupStateKey> = Vec::new();
		for (&(_, rn), entry) in slots.iter().zip(entries) {
			if let Some((time, source_rn)) = entry {
				removes.push(self.forward_key(source_rn));
				self.push_row_entry_removes(&mut removes, partition, rn, time);
			}
			txn.remove_unobserved(&self.rb_key(object_id, rn, partition))?;
		}
		self.state_remove_many(txn, &removes)?;

		let new_head = self.lowest_storage_row(txn, partition)?;

		match partition_values.is_empty() {
			true => {
				let mut meta = self.read_metadata(txn)?;
				meta.count = meta.count.saturating_sub(evicted_count);
				if let Some(head) = new_head {
					meta.head = head;
				}
				self.write_metadata(txn, &meta)?;
			}
			false => {
				let values = partition_values.to_vec();
				let mut meta = self.read_partition_metadata(txn, &values)?;
				meta.count = meta.count.saturating_sub(evicted_count);
				match new_head {
					Some(head) if !meta.is_empty() => {
						meta.head = head;
						self.write_partition_metadata(txn, &values, &meta)?;
					}
					_ => self.remove_partition_metadata(txn, &values)?,
				}
			}
		}
		Ok(true)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_insert(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		metadata: &mut Option<RingBufferMetadata>,
		partition_metadata: &mut PartitionMetadata,
		post: &RecordBatch,
		touched: &mut IndexSet<Vec<Value>>,
	) -> Result<()> {
		if post.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(post, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.num_rows();
		let field_columns = shape_field_columns(source, shape);
		let encoder = SourceRowEncoder::new(source, shape, &field_columns)?;
		let times = row_times(source)?;
		let row_numbers = require_row_numbers(source)?;
		let encoded = encoder.encode_all()?;
		let mut row_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut values_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(row_count);

		if self.is_partitioned() {
			let parts = partition_batch(view, &self.partition_indices, source)?;
			resolve_partitions_flow(txn, object_id.into(), &parts.distinct, &mut self.verified_partitions)?;
			let group_index: HashMap<Partition, usize> = parts
				.distinct
				.iter()
				.enumerate()
				.map(|(group, (partition, _))| (*partition, group))
				.collect();
			let mut group_rows: Vec<Vec<usize>> = vec![Vec::new(); parts.distinct.len()];
			for (row_idx, partition) in parts.rows.iter().enumerate() {
				group_rows[group_index[partition]].push(row_idx);
			}
			for ((partition, values), rows) in parts.distinct.into_iter().zip(group_rows) {
				note_touched(touched, &values);
				let meta = self.partition_meta(txn, partition_metadata, &values)?;
				self.insert_group(
					txn,
					object_id,
					meta,
					Some(partition),
					&encoded,
					row_numbers,
					&times,
					&rows,
					&mut row_keys,
					&mut values_bytes_vec,
				)?;
			}
		} else {
			note_touched(touched, &[]);
			let meta = metadata
				.as_mut()
				.expect("non-partitioned ring buffer sink must have loaded global metadata");
			let rows: Vec<usize> = (0..row_count).collect();
			self.insert_group(
				txn,
				object_id,
				meta,
				None,
				&encoded,
				row_numbers,
				&times,
				&rows,
				&mut row_keys,
				&mut values_bytes_vec,
			)?;
		}

		txn.set(row_keys, values_bytes_vec)?;
		txn.emit(view.id(), Diff::insert(coerced))?;
		Ok(())
	}

	fn partition_meta<'a>(
		&self,
		txn: &mut DeferredTransaction,
		partition_metadata: &'a mut PartitionMetadata,
		values: &[Value],
	) -> Result<&'a mut RingBufferMetadata> {
		if !partition_metadata.contains_key(values) {
			let loaded = self.read_partition_metadata(txn, values)?;
			partition_metadata.insert(values.to_vec(), (loaded.clone(), loaded));
		}
		Ok(&mut partition_metadata.get_mut(values).expect("partition metadata was just loaded").1)
	}

	#[allow(clippy::too_many_arguments)]
	fn insert_group(
		&self,
		txn: &mut DeferredTransaction,
		object_id: StorageId,
		meta: &mut RingBufferMetadata,
		partition: Option<Partition>,
		encoded: &[EncodedBytes],
		row_numbers: &[RowNumber],
		times: &[Option<DateTime>],
		rows: &[usize],
		row_keys: &mut Vec<EncodedKey>,
		values: &mut Vec<EncodedBytes>,
	) -> Result<()> {
		let incoming = rows.len() as u64;
		let mut evict_needed = (meta.count + incoming).saturating_sub(self.capacity);
		let mut removes: Vec<GroupStateKey> = Vec::new();

		while evict_needed > 0 && meta.head < meta.tail {
			let window: Vec<(Option<Partition>, RowNumber)> = (meta.head
				..meta.tail.min(meta.head + evict_needed))
				.map(|rn| (partition, RowNumber(rn)))
				.collect();
			let entries = self.row_entries(txn, &window)?;
			let keys: Vec<EncodedKey> =
				window.iter().map(|&(_, rn)| self.rb_key(object_id, rn, partition)).collect();
			let stored = txn.get_many(&keys)?;
			for (((&(_, rn), entry), key), row) in window.iter().zip(entries).zip(keys).zip(stored) {
				meta.head += 1;
				if let Some((time, source_rn)) = entry {
					removes.push(self.forward_key(source_rn));
					self.push_row_entry_removes(&mut removes, partition, rn, time);
				}
				if row.is_none() {
					continue;
				}
				txn.remove_one(&key)?;
				meta.count = meta.count.saturating_sub(1);
				evict_needed -= 1;
			}
		}
		self.state_remove_many(txn, &removes)?;

		let skip = evict_needed.min(incoming) as usize;
		meta.tail += skip as u64;

		let mut sets: Vec<(GroupStateKey, EncodedPodRow)> = Vec::with_capacity((rows.len() - skip) * 3);
		for &row_idx in &rows[skip..] {
			let source_rn = row_numbers[row_idx];
			let assigned_rn = RowNumber(meta.tail);
			self.push_row_entry_sets(
				&mut sets,
				partition,
				assigned_rn,
				source_rn,
				times.get(row_idx).copied().flatten(),
			)?;
			row_keys.push(self.rb_key(object_id, assigned_rn, partition));
			values.push(encoded[row_idx].clone());
			if meta.is_empty() {
				meta.head = assigned_rn.0;
			}
			meta.count += 1;
			meta.tail = assigned_rn.0 + 1;
		}
		self.state_set_many(txn, sets)
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_update(
		&mut self,
		txn: &mut DeferredTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		pre: &RecordBatch,
		post: &RecordBatch,
		touched: &mut IndexSet<Vec<Value>>,
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
		let row_count = source_post.num_rows();
		let mut applied: Vec<usize> = Vec::with_capacity(row_count);
		let pre_row_numbers = require_row_numbers(source_pre)?;
		let post_parts = if self.is_partitioned() {
			let pre_parts = partition_batch(view, &self.partition_indices, source_pre)?;
			let post_parts = partition_batch(view, &self.partition_indices, source_post)?;
			for (&pre_partition, &post_partition) in pre_parts.rows.iter().zip(&post_parts.rows) {
				ensure_partition_unchanged(object_id.into(), pre_partition, post_partition)?;
			}
			resolve_partitions_flow(
				txn,
				object_id.into(),
				&post_parts.distinct,
				&mut self.verified_partitions,
			)?;
			Some(post_parts)
		} else {
			None
		};
		let post_values: HashMap<Partition, &Vec<Value>> = post_parts
			.iter()
			.flat_map(|parts| parts.distinct.iter().map(|(partition, values)| (*partition, values)))
			.collect();
		let mut targets: Vec<(EncodedKey, Option<EncodedKey>)> = Vec::with_capacity(row_count);
		let forwards = self.forwards(txn, pre_row_numbers)?;
		for (row_idx, forward) in forwards.into_iter().enumerate() {
			let partition = if let Some(parts) = &post_parts {
				let post_partition = parts.rows[row_idx];
				note_touched(touched, post_values[&post_partition]);
				Some(post_partition)
			} else {
				note_touched(touched, &[]);
				None
			};

			let Some(storage_rn) = forward else {
				continue;
			};
			targets.push((self.rb_key(object_id, storage_rn, partition), None));
			applied.push(row_idx);
		}
		let mut created = created_at(source_post)?.to_vec();
		for (&row_idx, stored) in applied.iter().zip(stored_created_at_many(txn, &targets)?) {
			if let (Some(stamp), Some(slot)) = (stored, created.get_mut(row_idx)) {
				*slot = stamp;
			}
		}
		let field_columns = shape_field_columns(source_post, shape);
		let encoded = SourceRowEncoder::new(source_post, shape, &field_columns)?
			.with_created_at(&created)
			.encode_all()?;
		for ((key, _), &row_idx) in targets.iter().zip(&applied) {
			txn.set_one(key, encoded[row_idx].clone())?;
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
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_remove(
		&self,
		txn: &mut DeferredTransaction,
		view: &View,
		object_id: StorageId,
		metadata: &mut Option<RingBufferMetadata>,
		partition_metadata: &mut PartitionMetadata,
		pre: &RecordBatch,
		touched: &mut IndexSet<Vec<Value>>,
	) -> Result<()> {
		if pre.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(pre, view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_lookup_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = coerced.num_rows();
		let mut applied: Vec<usize> = Vec::with_capacity(row_count);
		let row_numbers = require_row_numbers(&coerced)?;
		let parts = if self.is_partitioned() {
			Some(partition_batch(view, &self.partition_indices, source)?)
		} else {
			None
		};
		let part_values: HashMap<Partition, &Vec<Value>> = parts
			.iter()
			.flat_map(|parts| parts.distinct.iter().map(|(partition, values)| (*partition, values)))
			.collect();
		let forwards = self.forwards(txn, row_numbers)?;
		let mut seen: HashSet<RowNumber> = HashSet::with_capacity(row_count);
		let mut found: Vec<(usize, RowNumber, RowNumber, Option<Partition>)> = Vec::with_capacity(row_count);
		for (row_idx, (&source_rn, forward)) in row_numbers.iter().zip(forwards).enumerate() {
			let Some(storage_rn) = forward else {
				continue;
			};
			if !seen.insert(source_rn) {
				continue;
			}
			let partition = parts.as_ref().map(|parts| parts.rows[row_idx]);
			found.push((row_idx, source_rn, storage_rn, partition));
		}
		let slots: Vec<(Option<Partition>, RowNumber)> =
			found.iter().map(|&(_, _, storage_rn, partition)| (partition, storage_rn)).collect();
		let entries = self.row_entries(txn, &slots)?;
		let mut removes: Vec<GroupStateKey> = Vec::with_capacity(found.len() * 3);
		for (&(row_idx, source_rn, storage_rn, partition), entry) in found.iter().zip(entries) {
			let partition_values = partition.map(|partition| part_values[&partition]);
			note_touched(touched, partition_values.map_or(&[][..], Vec::as_slice));

			removes.push(self.forward_key(source_rn));
			if let Some((time, _)) = entry {
				self.push_row_entry_removes(&mut removes, partition, storage_rn, time);
			}

			let key = self.rb_key(object_id, storage_rn, partition);
			txn.remove_one(&key)?;

			let meta = match partition_values {
				Some(partition_values) => {
					self.partition_meta(txn, partition_metadata, partition_values)?
				}
				None => metadata
					.as_mut()
					.expect("non-partitioned ring buffer sink must have loaded global metadata"),
			};
			meta.count = meta.count.saturating_sub(1);
			applied.push(row_idx);
		}
		self.state_remove_many(txn, &removes)?;
		if !applied.is_empty() {
			txn.emit(view.id(), Diff::remove(JoinedColumnsBuilder::retain_rows(&coerced, &applied)?))?;
		}
		Ok(())
	}
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, UInt64Array};
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::CommitVersion,
		delta::RemoveVisibility,
		interface::{
			catalog::{
				column::{Column as CatalogColumn, ColumnIndex},
				id::{ColumnId, NamespaceId, ViewId},
				namespace::Namespace,
				view::{RingBufferView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
		key::any::TaggedKey,
		value::{
			batch::batch,
			column::factory::{int4, utf8},
		},
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			constraint::TypeConstraint,
			container::temporal_array::datetime_array,
			datetime::DateTime,
			identity::IdentityId,
			system_columns::{SystemColumn, with_system_column},
			value_type::ValueType,
		},
	};

	use super::*;
	use crate::transaction::{
		deferred::StorageCounts, mock::FlowTxn, scope::scoped_key, substrate::apply_operator_state,
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
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		for (key, pw) in pending.rows_ordered() {
			let key = TaggedKey::decode(key).unwrap();
			match pw {
				PendingWrite::Set(v) => cmd.set(&key, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced,
				} => cmd.remove(&key).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Unobserved,
				} => cmd.remove_unobserved(&key).unwrap(),
				PendingWrite::Remove {
					announce: RemoveVisibility::Silent,
				} => cmd.remove_silent(&key).unwrap(),
			};
		}
		cmd.commit().unwrap();
		apply_operator_state(&engine.inner().operator_state(), &pending);
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
		let key = op.timer_key(partition_values);
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

	fn partition_prefix(values: &[Value]) -> Vec<u8> {
		row_entry_prefix((!values.is_empty()).then(|| Partition::of(values)))
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
			(op.expiry_key(None, 7, RowNumber(42)), KeyspaceId::RINGBUFFER_EXPIRY),
			(op.arm_key(None), KeyspaceId::RINGBUFFER_TTL_ARM),
			(op.meta_key(None), KeyspaceId::RINGBUFFER_META),
			(op.row_entry_key(Some(partition), RowNumber(42)), KeyspaceId::PARTITIONED_RINGBUFFER_ENTRY),
			(op.expiry_key(Some(partition), 7, RowNumber(42)), KeyspaceId::PARTITIONED_RINGBUFFER_EXPIRY),
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
		let mut txn = deferred_txn(engine);
		let prefix = partition_prefix(values);
		op.state_range(&mut txn, EncodedKeyRange::prefix(&prefix)).collect::<Result<Vec<_>>>().unwrap().len()
	}

	fn forward_count(engine: &TestEngine, op: &SinkRingBufferViewOperator) -> usize {
		let mut txn = deferred_txn(engine);
		let prefix = OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::RINGBUFFER_FORWARD, vec![]);
		op.state_range(&mut txn, EncodedKeyRange::prefix(prefix.as_ref()))
			.collect::<Result<Vec<_>>>()
			.unwrap()
			.len()
	}

	fn metadata(engine: &TestEngine, values: &[Value]) -> Option<RingBufferMetadata> {
		let mut txn = deferred_txn(engine);
		let key = if values.is_empty() {
			RingBufferMetadataKey::encoded(RB)
		} else {
			RingBufferMetadataKey::encoded_partition(RB, values.to_vec())
		};
		txn.get(&key).unwrap().map(|row| decode_ringbuffer_metadata(EncodedPodRow::view(&row)).unwrap())
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
		let key = op.timer_key(&base("us"));
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
	fn partial_expiry_decrements_count_and_advances_head_to_the_survivor() {
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &mut op, true, &[("us", 3), ("us", 4)], 3, AFTER);

		let before = metadata(&engine, &base("us")).unwrap();
		assert_eq!(before.count, 4);
		let survivor_head = before.head + 2;

		fire(&engine, &mut op, &base("us"), AFTER);

		let after = metadata(&engine, &base("us")).expect("partition still has survivors");
		assert_eq!(after.count, 2, "the two expired rows must be subtracted");
		assert_eq!(after.head, survivor_head, "head must advance to the oldest surviving row");
		assert_eq!(after.tail, before.tail, "tail must not move on eviction");
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
	fn min_survivor_head_is_correct_when_an_out_of_order_row_expires_first() {
		// In event time the expired rows are not a prefix of the ring: an out-of-order arrival
		// can put the first row to expire physically after a survivor, so the head has to be
		// recomputed from what is left rather than advanced by the evicted count.
		let engine = TestEngine::new();
		let mut op = build_op(true, Some(hour_ttl()));

		insert_at(&engine, &mut op, true, &[("us", 1)], 1, AFTER);
		let head_before = metadata(&engine, &base("us")).unwrap().head;
		insert_at(&engine, &mut op, true, &[("us", 2)], 2, T0);

		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2, "precondition: both rows are live");

		fire(&engine, &mut op, &base("us"), AFTER);

		let after = metadata(&engine, &base("us")).expect("the fresh row survives");
		assert_eq!(after.count, 1, "only the row whose own time already expired is evicted");
		assert_eq!(after.head, head_before, "head stays at the surviving row, the true min survivor");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 1);
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
			assert_eq!(
				eviction_counts(partitioned, 3),
				eviction_counts(partitioned, 1),
				"evicting more rows must not add storage reads (partitioned: {partitioned})"
			);
		}
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
			assert_eq!(
				remove_counts(partitioned, 3),
				remove_counts(partitioned, 1),
				"removing more rows must not add storage reads (partitioned: {partitioned})"
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
		let key = op.timer_key(&partition);
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
}
