// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, collections::HashMap, ops::Bound};

use reifydb_abi::operator::{capabilities::OperatorCapability, timer::TimerKind};
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::{
		decode_u64_asc, encode_u64_asc, encode_u128_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
};
use reifydb_core::{
	interface::{
		catalog::{
			flow::OperatorId,
			id::RingBufferId,
			object::ObjectId,
			ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata, encode_ringbuffer_metadata},
			storage::StorageId,
			view::View,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::{
		EncodableKey,
		operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey},
		partitioned_row::{PartitionedRowKey, RowLocator},
		ringbuffer::RingBufferMetadataKey,
		row::RowKey,
	},
	row::row_shape_from_columns,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::partition::partition_col_indices;
use reifydb_flow::{operator::Operator, timer::Timer, transaction::FlowTransaction};
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	util::cowvec::CowVec,
	value::{
		Value, blob::Blob, datetime::DateTime, duration::Duration, partition::Partition, row_number::RowNumber,
		system_columns::SystemColumns, value_type::ValueType,
	},
};
use smallvec::smallvec;

use super::{
	coerce_columns, decode_dictionary_columns, encode_row_at_index,
	partition::{ensure_partition_unchanged, partition_of, resolve_partition_flow},
	shape_field_columns,
	view::dictionary_encode_view_columns,
};
use crate::{
	error::FlowStateError,
	operator::{OperatorCell, join::column::JoinedColumnsBuilder, stateful::raw::RawStatefulOperator},
};

fn partition_suffix(partition: Option<Partition>) -> Vec<u8> {
	match partition {
		Some(partition) => encode_u128_asc(partition.0).to_vec(),
		None => Vec::new(),
	}
}

fn row_entry_prefix(partition: Option<Partition>) -> Vec<u8> {
	let mut prefix = OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::RINGBUFFER_ENTRY, [])
		.as_slice()
		.to_vec();
	prefix.extend_from_slice(&partition_suffix(partition));
	prefix
}

fn expiry_scan_prefix(partition: Option<Partition>) -> Vec<u8> {
	let mut prefix = OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::RINGBUFFER_EXPIRY, [])
		.as_slice()
		.to_vec();
	prefix.extend_from_slice(&partition_suffix(partition));
	prefix
}

fn note_touched(touched: &mut Vec<Vec<Value>>, partition_values: Vec<Value>) {
	if !touched.contains(&partition_values) {
		touched.push(partition_values);
	}
}

fn partition_of_values(partition_values: &[Value]) -> Option<Partition> {
	(!partition_values.is_empty()).then(|| Partition::of(partition_values))
}

fn decode_expiry_key(bytes: &[u8]) -> Result<(u64, u64)> {
	if bytes.len() < 16 {
		return Err(Error::from(FlowStateError::Decode {
			state: "RingBufferExpiry",
			cause: "expiry key shorter than 16 bytes".to_string(),
		}));
	}
	let tail = &bytes[bytes.len() - 16..];
	let expires_at = decode_u64_asc(tail[..8].try_into().expect("a 16-byte tail has an 8-byte head"));
	let storage_rn = decode_u64_asc(tail[8..].try_into().expect("a 16-byte tail has an 8-byte tail"));
	Ok((expires_at, storage_rn))
}

pub struct SinkRingBufferViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	operator: OperatorId,
	view: ResolvedView,
	ringbuffer_id: RingBufferId,
	capacity: u64,
	announce_evictions: bool,
	ttl: Option<Duration>,
	state_shape: RowShape,
	partition_indices: Vec<usize>,
	verified_partitions: UnsafeCell<HashMap<Partition, Vec<Value>>>,
}

impl SinkRingBufferViewOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		parent: OperatorCell,
		operator: OperatorId,
		view: ResolvedView,
		ringbuffer_id: RingBufferId,
		capacity: u64,
		announce_evictions: bool,
		ttl: Option<Duration>,
		partition_by: Vec<String>,
	) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		Self {
			parent,
			operator,
			view,
			ringbuffer_id,
			capacity,
			announce_evictions,
			ttl,
			state_shape: RowShape::operator_state(),
			partition_indices,
			verified_partitions: UnsafeCell::new(HashMap::new()),
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	#[allow(clippy::mut_from_ref)]
	fn verified_partitions(&self) -> &mut HashMap<Partition, Vec<Value>> {
		// SAFETY: an operator is reachable from one thread at a time and each apply helper takes this
		// borrow once, calling nothing that reaches the cell again, so no other borrow is live.
		unsafe { &mut *self.verified_partitions.get() }
	}

	#[inline]
	fn rb_key(&self, object_id: StorageId, rn: RowNumber, partition: Option<Partition>) -> EncodedKey {
		match partition {
			Some(partition) => PartitionedRowKey::encoded(object_id, partition, RowLocator::Row(rn)),
			None => RowKey::encoded(object_id, rn),
		}
	}

	fn meta_key(&self, partition: Option<Partition>) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::RINGBUFFER_META,
			partition_suffix(partition),
		)
	}

	fn read_meta_mirror(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		let key = self.meta_key(partition);
		let Some(row) = self.state_get(txn, &key)? else {
			return Ok(None);
		};
		let blob = self.state_shape.get_blob(&row, 0);
		Ok(Some(decode_ringbuffer_metadata(&EncodedRow(CowVec::new(blob.as_bytes().to_vec())))))
	}

	fn write_meta_mirror(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = self.meta_key(partition);
		let mut row = self.state_shape.allocate();
		self.state_shape.set_blob(&mut row, 0, &Blob::from(encode_ringbuffer_metadata(metadata).to_vec()));
		self.state_set(txn, &key, row.freeze())
	}

	#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
	fn assert_mirrors_mvcc(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		mvcc: &EncodedKey,
	) -> Result<()> {
		reifydb_assertions! {
			let mirrored = self.read_meta_mirror(txn, partition)?;
			let stored = txn.get(mvcc)?.map(|row| decode_ringbuffer_metadata(&row));
			assert_eq!(
				mirrored, stored,
				"the ringbuffer metadata mirror and its mvcc row disagree right after a write; the \
				 mirror is what replay reads, so any drift hands catch-up a different tail than the \
				 live run assigned and the arena forward map diverges silently"
			);
		}
		Ok(())
	}

	fn read_metadata(&self, txn: &mut FlowTransaction) -> Result<RingBufferMetadata> {
		if let Some(metadata) = self.read_meta_mirror(txn, None)? {
			return Ok(metadata);
		}
		let key = RingBufferMetadataKey::encoded(self.ringbuffer_id);
		let metadata = match txn.get(&key)? {
			Some(row) => decode_ringbuffer_metadata(&row),
			None => RingBufferMetadata::new(self.ringbuffer_id, self.capacity),
		};
		self.write_meta_mirror(txn, None, &metadata)?;
		Ok(metadata)
	}

	fn write_metadata(&self, txn: &mut FlowTransaction, metadata: &RingBufferMetadata) -> Result<()> {
		let key = RingBufferMetadataKey::encoded(self.ringbuffer_id);
		let row = encode_ringbuffer_metadata(metadata);
		txn.set(&key, row)?;
		self.write_meta_mirror(txn, None, metadata)?;
		self.assert_mirrors_mvcc(txn, None, &key)
	}

	fn read_partition_metadata(
		&self,
		txn: &mut FlowTransaction,
		partition_values: &[Value],
	) -> Result<RingBufferMetadata> {
		let partition = partition_of_values(partition_values);
		if let Some(metadata) = self.read_meta_mirror(txn, partition)? {
			return Ok(metadata);
		}
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		let metadata = match txn.get(&key)? {
			Some(row) => decode_ringbuffer_metadata(&row),
			None => RingBufferMetadata::new(self.ringbuffer_id, self.capacity),
		};
		self.write_meta_mirror(txn, partition, &metadata)?;
		Ok(metadata)
	}

	fn write_partition_metadata(
		&self,
		txn: &mut FlowTransaction,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		let row = encode_ringbuffer_metadata(metadata);
		txn.set(&key, row)?;
		let partition = partition_of_values(partition_values);
		self.write_meta_mirror(txn, partition, metadata)?;
		self.assert_mirrors_mvcc(txn, partition, &key)
	}

	fn remove_partition_metadata(&self, txn: &mut FlowTransaction, partition_values: &[Value]) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		txn.remove(&key)?;
		let partition = partition_of_values(partition_values);
		self.state_remove(txn, &self.meta_key(partition))?;
		self.assert_mirrors_mvcc(txn, partition, &key)
	}

	fn forward_key(&self, source_rn: RowNumber) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::RINGBUFFER_FORWARD,
			encode_u64_asc(source_rn.0),
		)
	}

	fn get_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber) -> Result<Option<RowNumber>> {
		let key = self.forward_key(source_rn);
		match self.state_get(txn, &key)? {
			Some(row) => Ok(Some(self.decode_row_number(&row, "RingBufferForward")?)),
			None => Ok(None),
		}
	}

	fn set_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber, storage_rn: RowNumber) -> Result<()> {
		let key = self.forward_key(source_rn);
		let mut row = self.state_shape.allocate();
		self.state_shape.set_blob(&mut row, 0, &Blob::from(storage_rn.0.to_be_bytes().to_vec()));
		self.state_set(txn, &key, row.freeze())
	}

	fn drop_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber) -> Result<()> {
		let key = self.forward_key(source_rn);
		self.state_remove(txn, &key)
	}

	fn row_entry_key(&self, partition: Option<Partition>, storage_rn: RowNumber) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(24);
		if let Some(partition) = partition {
			suffix.extend_from_slice(&encode_u128_asc(partition.0));
		}
		suffix.extend_from_slice(&encode_u64_asc(storage_rn.0));
		OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::RINGBUFFER_ENTRY, suffix)
	}

	fn expiry_key(&self, partition: Option<Partition>, expires_at: u64, storage_rn: RowNumber) -> GroupStateKey {
		let mut suffix = partition_suffix(partition);
		suffix.extend_from_slice(&encode_u64_asc(expires_at));
		suffix.extend_from_slice(&encode_u64_asc(storage_rn.0));
		OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::RINGBUFFER_EXPIRY, suffix)
	}

	fn arm_key(&self, partition: Option<Partition>) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::RINGBUFFER_TTL_ARM,
			partition_suffix(partition),
		)
	}

	fn expires_at(&self, time: Option<DateTime>) -> Option<u64> {
		let ttl = self.ttl?;
		let time = time?;
		let ttl_ms = u64::try_from(ttl.milliseconds().ok()?).ok()?;
		Some(time.to_millis().saturating_add(ttl_ms))
	}

	fn set_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
		source_rn: RowNumber,
		time: Option<DateTime>,
	) -> Result<()> {
		let key = self.row_entry_key(partition, storage_rn);
		let mut row = self.state_shape.allocate();
		let mut payload = Vec::with_capacity(16);
		payload.extend_from_slice(&source_rn.0.to_be_bytes());
		if let Some(time) = time {
			payload.extend_from_slice(&time.to_millis().to_be_bytes());
		}
		self.state_shape.set_blob(&mut row, 0, &Blob::from(payload));
		self.state_set(txn, &key, row.freeze())?;
		if let Some(expires_at) = self.expires_at(time) {
			let key = self.expiry_key(partition, expires_at, storage_rn);
			self.state_set(txn, &key, self.state_shape.allocate().freeze())?;
		}
		Ok(())
	}

	fn readdress_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
		source_rn: RowNumber,
	) -> Result<()> {
		let key = self.row_entry_key(partition, storage_rn);
		let Some(row) = self.state_get(txn, &key)? else {
			return Ok(());
		};
		let (time, _) = self.decode_row_entry(&row)?;
		self.set_row_entry(txn, partition, storage_rn, source_rn, time)
	}

	fn drop_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
	) -> Result<()> {
		let key = self.row_entry_key(partition, storage_rn);
		let Some(row) = self.state_get(txn, &key)? else {
			return Ok(());
		};
		let (time, _) = self.decode_row_entry(&row)?;
		if let Some(expires_at) = self.expires_at(time) {
			self.state_remove(txn, &self.expiry_key(partition, expires_at, storage_rn))?;
		}
		self.state_remove(txn, &key)
	}

	fn take_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
	) -> Result<Option<RowNumber>> {
		let key = self.row_entry_key(partition, storage_rn);
		match self.state_get(txn, &key)? {
			Some(row) => {
				let (time, source_rn) = self.decode_row_entry(&row)?;
				self.drop_forward(txn, source_rn)?;
				if let Some(expires_at) = self.expires_at(time) {
					self.state_remove(txn, &self.expiry_key(partition, expires_at, storage_rn))?;
				}
				self.state_remove(txn, &key)?;
				Ok(Some(source_rn))
			}
			None => Ok(None),
		}
	}

	fn decode_row_entry(&self, row: &EncodedRow) -> Result<(Option<DateTime>, RowNumber)> {
		let blob = self.state_shape.get_blob(row, 0);
		let bytes = blob.as_bytes();
		if bytes.len() != 8 && bytes.len() != 16 {
			return Err(Error::from(FlowStateError::Decode {
				state: "RingBufferRowEntry",
				cause: format!("expected 8 or 16 bytes, got {}", bytes.len()),
			}));
		}
		let source_rn = u64::from_be_bytes(
			bytes[..8].try_into().expect("a row entry opens with an 8-byte source row number"),
		);
		let time = (bytes.len() == 16).then(|| {
			DateTime::from_millis(u64::from_be_bytes(
				bytes[8..].try_into().expect("a 16-byte row entry closes with an 8-byte instant"),
			))
		});
		Ok((time, RowNumber(source_rn)))
	}

	fn decode_row_number(&self, row: &EncodedRow, state: &'static str) -> Result<RowNumber> {
		let blob = self.state_shape.get_blob(row, 0);
		let bytes: [u8; 8] = blob.as_bytes().try_into().map_err(|_| {
			Error::from(FlowStateError::Decode {
				state,
				cause: "expected 8 bytes".to_string(),
			})
		})?;
		Ok(RowNumber(u64::from_be_bytes(bytes)))
	}
}

impl RawStatefulOperator for SinkRingBufferViewOperator {}

impl Operator for SinkRingBufferViewOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape = row_shape_from_columns(view.columns());
		let object_id = StorageId::ringbuffer(self.ringbuffer_id);
		let mut metadata = if self.is_partitioned() {
			None
		} else {
			Some(self.read_metadata(txn)?)
		};
		let mut partition_metadata: HashMap<Vec<Value>, RingBufferMetadata> = HashMap::new();
		let mut touched: Vec<Vec<Value>> = Vec::new();

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_ringbuffer_insert(
					txn,
					&view,
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
					&view,
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
					&view,
					object_id,
					&mut metadata,
					&mut partition_metadata,
					pre,
					&mut touched,
				)?,
			}
		}

		if let Some(metadata) = &metadata {
			self.write_metadata(txn, metadata)?;
		}
		for (partition_values, partition_meta) in partition_metadata.iter() {
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

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		if timer.kind != TimerKind::RowTtl || self.ttl.is_none() {
			return Ok(None);
		}
		let partition_values = self.timer_partition_values(&timer.key)?;

		let view = self.view.def().clone();
		let shape = row_shape_from_columns(view.columns());
		let object_id = StorageId::ringbuffer(self.ringbuffer_id);
		let mut evicted_rns: Vec<RowNumber> = Vec::new();
		let mut evicted_rows: Vec<EncodedRow> = Vec::new();

		self.evict_due(
			txn,
			object_id,
			&partition_values,
			timer.at.to_millis(),
			&mut evicted_rns,
			&mut evicted_rows,
		)?;
		self.sync_row_ttl_timer(txn, &partition_values)?;

		if let Some(diff) = self.build_evicted_diff(txn, &view, &shape, evicted_rns, evicted_rows)? {
			emit_view_change(txn, &view, diff);
			let version = txn.version();
			return Ok(Some(Change::from_flow(self.operator, version, Vec::new(), timer.at)));
		}
		Ok(None)
	}
}

impl SinkRingBufferViewOperator {
	fn due_storage_rows(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		at: u64,
	) -> Result<Vec<u64>> {
		let prefix = expiry_scan_prefix(partition);
		let mut end = prefix.clone();
		end.extend_from_slice(&encode_u64_asc(at.saturating_add(1)));
		let range = EncodedKeyRange::new(
			Bound::Included(EncodedKey::new(prefix)),
			Bound::Excluded(EncodedKey::new(end)),
		);
		self.state_range(txn, range)
			.map(|result| {
				let (key, _row) = result?;
				Ok(decode_expiry_key(key.as_ref())?.1)
			})
			.collect()
	}

	fn lowest_storage_row(&self, txn: &mut FlowTransaction, partition: Option<Partition>) -> Result<Option<u64>> {
		let range = EncodedKeyRange::prefix(&row_entry_prefix(partition));
		match self.state_range(txn, range).next() {
			Some(result) => {
				let (key, _row) = result?;
				let bytes = key.as_ref();
				let rn: [u8; 8] = bytes[bytes.len() - 8..].try_into().map_err(|_| {
					Error::from(FlowStateError::Decode {
						state: "RingBufferRowEntry",
						cause: "row-entry key shorter than 8 bytes".to_string(),
					})
				})?;
				Ok(Some(decode_u64_asc(rn)))
			}
			None => Ok(None),
		}
	}

	fn earliest_expiry(&self, txn: &mut FlowTransaction, partition: Option<Partition>) -> Result<Option<u64>> {
		let range = EncodedKeyRange::prefix(&expiry_scan_prefix(partition));
		match self.state_range(txn, range).next() {
			Some(result) => {
				let (key, _row) = result?;
				Ok(Some(decode_expiry_key(key.as_ref())?.0))
			}
			None => Ok(None),
		}
	}

	fn timer_key(&self, partition_values: &[Value]) -> EncodedKey {
		match partition_values.is_empty() {
			true => RingBufferMetadataKey::encoded(self.ringbuffer_id),
			false => {
				RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec())
			}
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

	fn read_armed(&self, txn: &mut FlowTransaction, partition: Option<Partition>) -> Result<Option<u64>> {
		match self.state_get(txn, &self.arm_key(partition))? {
			Some(row) => Ok(Some(self.decode_row_number(&row, "RingBufferTtlArm")?.0)),
			None => Ok(None),
		}
	}

	fn sync_row_ttl_timer(&self, txn: &mut FlowTransaction, partition_values: &[Value]) -> Result<()> {
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
					at: DateTime::from_millis(at),
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
						at: DateTime::from_millis(at),
						kind: TimerKind::RowTtl,
						key,
					},
				)?;
				let mut row = self.state_shape.allocate();
				self.state_shape.set_blob(&mut row, 0, &Blob::from(at.to_be_bytes().to_vec()));
				self.state_set(txn, &arm_key, row.freeze())
			}
			None => self.state_remove(txn, &arm_key),
		}
	}

	fn evict_due(
		&self,
		txn: &mut FlowTransaction,
		object_id: StorageId,
		partition_values: &[Value],
		at: u64,
		evicted_rns: &mut Vec<RowNumber>,
		evicted_rows: &mut Vec<EncodedRow>,
	) -> Result<()> {
		let partition = partition_of_values(partition_values);

		let to_evict = self.due_storage_rows(txn, partition, at)?;
		if to_evict.is_empty() {
			return Ok(());
		}

		let evicted_count = to_evict.len() as u64;
		for storage_rn in to_evict {
			let rn = RowNumber(storage_rn);
			let pre_key = self.rb_key(object_id, rn, partition);
			let row = txn.get(&pre_key)?;
			let source_rn = self.take_row_entry(txn, partition, rn)?;
			if self.announce_evictions {
				if let Some(row) = row {
					evicted_rns.push(source_rn.unwrap_or(rn));
					evicted_rows.push(row);
				}
				txn.remove(&pre_key)?;
			} else {
				txn.remove_silent(&pre_key)?;
			}
		}

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
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_insert(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		metadata: &mut Option<RingBufferMetadata>,
		partition_metadata: &mut HashMap<Vec<Value>, RingBufferMetadata>,
		post: &Columns,
		touched: &mut Vec<Vec<Value>>,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, shape);
		let mut evicted_rns: Vec<RowNumber> = Vec::new();
		let mut evicted_rows: Vec<EncodedRow> = Vec::new();
		let mut row_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut row_values: Vec<EncodedRow> = Vec::with_capacity(row_count);

		if self.is_partitioned() {
			let verified = self.verified_partitions();
			let mut groups: Vec<(Partition, Vec<Value>, Vec<usize>)> = Vec::new();
			let mut group_index: HashMap<Partition, usize> = HashMap::new();
			for row_idx in 0..row_count {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
				match group_index.get(&partition) {
					Some(&group) => groups[group].2.push(row_idx),
					None => {
						group_index.insert(partition, groups.len());
						groups.push((partition, values, vec![row_idx]));
					}
				}
			}
			for (partition, values, rows) in groups {
				note_touched(touched, values.clone());
				resolve_partition_flow(txn, object_id.into(), partition, &values, verified)?;
				if !partition_metadata.contains_key(&values) {
					let loaded = self.read_partition_metadata(txn, &values)?;
					partition_metadata.insert(values.clone(), loaded);
				}
				let meta = partition_metadata.get_mut(&values).unwrap();
				self.insert_group(
					txn,
					object_id,
					meta,
					Some(partition),
					source,
					shape,
					&field_columns,
					&rows,
					&mut evicted_rns,
					&mut evicted_rows,
					&mut row_keys,
					&mut row_values,
				)?;
			}
		} else {
			note_touched(touched, Vec::new());
			let meta = metadata
				.as_mut()
				.expect("non-partitioned ring buffer sink must have loaded global metadata");
			let rows: Vec<usize> = (0..row_count).collect();
			self.insert_group(
				txn,
				object_id,
				meta,
				None,
				source,
				shape,
				&field_columns,
				&rows,
				&mut evicted_rns,
				&mut evicted_rows,
				&mut row_keys,
				&mut row_values,
			)?;
		}

		txn.set_batch(&row_keys, &row_values)?;
		emit_view_change(txn, view, Diff::insert(coerced));

		if let Some(diff) = self.build_evicted_diff(txn, view, shape, evicted_rns, evicted_rows)? {
			emit_view_change(txn, view, diff);
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn insert_group(
		&self,
		txn: &mut FlowTransaction,
		object_id: StorageId,
		meta: &mut RingBufferMetadata,
		partition: Option<Partition>,
		source: &Columns,
		shape: &RowShape,
		field_columns: &[usize],
		rows: &[usize],
		evicted_rns: &mut Vec<RowNumber>,
		evicted_rows: &mut Vec<EncodedRow>,
		row_keys: &mut Vec<EncodedKey>,
		row_values: &mut Vec<EncodedRow>,
	) -> Result<()> {
		let incoming = rows.len() as u64;
		let mut evict_needed = (meta.count + incoming).saturating_sub(meta.capacity);

		while evict_needed > 0 && meta.head < meta.tail {
			let oldest_rn = RowNumber(meta.head);
			meta.head += 1;
			let source_rn = self.take_row_entry(txn, partition, oldest_rn)?;
			let pre_key = self.rb_key(object_id, oldest_rn, partition);
			let Some(row) = txn.get(&pre_key)? else {
				continue;
			};
			if self.announce_evictions {
				evicted_rns.push(source_rn.unwrap_or(oldest_rn));
				evicted_rows.push(row);
				txn.remove(&pre_key)?;
			} else {
				txn.remove_silent(&pre_key)?;
			}
			meta.count = meta.count.saturating_sub(1);
			evict_needed -= 1;
		}

		let skip = evict_needed.min(incoming) as usize;
		for &row_idx in &rows[..skip] {
			meta.tail += 1;
			if self.announce_evictions {
				let source_rn = source.row_numbers()[row_idx];
				let (_, encoded) =
					encode_row_at_index(source, row_idx, shape, source_rn, field_columns)?;
				evicted_rns.push(source_rn);
				evicted_rows.push(encoded);
			}
		}

		for &row_idx in &rows[skip..] {
			let source_rn = source.row_numbers()[row_idx];
			let assigned_rn = RowNumber(meta.tail);
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, assigned_rn, field_columns)?;
			self.set_forward(txn, source_rn, assigned_rn)?;
			self.set_row_entry(
				txn,
				partition,
				assigned_rn,
				source_rn,
				source.time().get(row_idx).copied(),
			)?;
			row_keys.push(self.rb_key(object_id, assigned_rn, partition));
			row_values.push(encoded);
			if meta.is_empty() {
				meta.head = assigned_rn.0;
			}
			meta.count += 1;
			meta.tail = assigned_rn.0 + 1;
		}
		Ok(())
	}

	fn build_evicted_diff(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		evicted_rns: Vec<RowNumber>,
		evicted_rows: Vec<EncodedRow>,
	) -> Result<Option<Diff>> {
		if !self.announce_evictions || evicted_rows.is_empty() {
			return Ok(None);
		}
		let storage_columns: Vec<ColumnWithName> = view
			.columns()
			.iter()
			.map(|col| {
				let ty = if col.dictionary_id.is_some() {
					ValueType::DictionaryId
				} else {
					col.constraint.get_type()
				};
				ColumnWithName {
					name: Fragment::internal(&col.name),
					data: ColumnBuffer::with_capacity(ty, 0),
				}
			})
			.collect();
		let mut evicted = Columns::with_system(storage_columns, SystemColumns::default());
		evicted.append_rows(shape, evicted_rows, evicted_rns)?;
		decode_dictionary_columns(&mut evicted, txn)?;
		Ok(Some(Diff::remove(evicted)))
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_update(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: StorageId,
		pre: &Columns,
		post: &Columns,
		touched: &mut Vec<Vec<Value>>,
	) -> Result<()> {
		let coerced_pre = coerce_columns(pre, view.columns())?;
		let coerced_post = coerce_columns(post, view.columns())?;
		let dict_pre = dictionary_encode_view_columns(txn, view, &coerced_pre)?;
		let dict_post = dictionary_encode_view_columns(txn, view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.row_count();
		let field_columns = shape_field_columns(source_post, shape);
		let verified = self.verified_partitions();
		let mut applied: Vec<usize> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_source_rn = source_pre.row_numbers()[row_idx];
			let post_source_rn = source_post.row_numbers()[row_idx];

			let partition = if self.is_partitioned() {
				let (pre_partition, _) = partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(object_id.into(), pre_partition, post_partition)?;
				resolve_partition_flow(txn, object_id.into(), post_partition, &post_values, verified)?;
				note_touched(touched, post_values);
				Some(post_partition)
			} else {
				note_touched(touched, Vec::new());
				None
			};

			let Some(storage_rn) = self.get_forward(txn, pre_source_rn)? else {
				continue;
			};
			let key = self.rb_key(object_id, storage_rn, partition);

			if post_source_rn != pre_source_rn {
				self.drop_forward(txn, pre_source_rn)?;
				self.set_forward(txn, post_source_rn, storage_rn)?;
				self.readdress_row_entry(txn, partition, storage_rn, post_source_rn)?;
			}

			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, storage_rn, &field_columns)?;
			txn.set(&key, post_encoded)?;
			applied.push(row_idx);
		}
		if !applied.is_empty() {
			emit_view_change(
				txn,
				view,
				Diff::update(
					JoinedColumnsBuilder::retain_rows(&coerced_pre, &applied),
					JoinedColumnsBuilder::retain_rows(&coerced_post, &applied),
				),
			);
		}
		Ok(())
	}

	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_remove(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		object_id: StorageId,
		metadata: &mut Option<RingBufferMetadata>,
		partition_metadata: &mut HashMap<Vec<Value>, RingBufferMetadata>,
		pre: &Columns,
		touched: &mut Vec<Vec<Value>>,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		let mut applied: Vec<usize> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_rn = coerced.row_numbers()[row_idx];
			let Some(storage_rn) = self.get_forward(txn, source_rn)? else {
				continue;
			};

			let (partition, partition_values) = if self.is_partitioned() {
				let (partition, partition_values) =
					partition_of(&self.partition_indices, &coerced, row_idx);
				note_touched(touched, partition_values.clone());
				(Some(partition), Some(partition_values))
			} else {
				note_touched(touched, Vec::new());
				(None, None)
			};

			self.drop_forward(txn, source_rn)?;
			self.drop_row_entry(txn, partition, storage_rn)?;

			let key = self.rb_key(object_id, storage_rn, partition);
			txn.remove(&key)?;

			if let Some(partition_values) = partition_values {
				if !partition_metadata.contains_key(&partition_values) {
					let loaded = self.read_partition_metadata(txn, &partition_values)?;
					partition_metadata.insert(partition_values.clone(), loaded);
				}
				let pm = partition_metadata
					.get_mut(&partition_values)
					.expect("partition metadata was just loaded");
				pm.count = pm.count.saturating_sub(1);
			} else {
				let meta = metadata
					.as_mut()
					.expect("non-partitioned ring buffer sink must have loaded global metadata");
				meta.count = meta.count.saturating_sub(1);
			}
			applied.push(row_idx);
		}
		if !applied.is_empty() {
			emit_view_change(
				txn,
				view,
				Diff::remove(JoinedColumnsBuilder::retain_rows(&coerced, &applied)),
			);
		}
		Ok(())
	}
}

#[inline]
fn emit_view_change(txn: &mut FlowTransaction, view: &View, diff: Diff) {
	let version = txn.version();
	let changed_at = txn.clock().now();
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		actors::pending::PendingWrite,
		common::CommitVersion,
		interface::{
			catalog::{
				column::{Column as CatalogColumn, ColumnIndex},
				id::{ColumnId, NamespaceId, TableId, ViewId},
				namespace::Namespace,
				view::{TableView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
		key::{Key, kind::KeyKind},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_flow::transaction::substrate::apply_operator_state;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::{constraint::TypeConstraint, datetime::DateTime, identity::IdentityId};

	use super::*;
	use crate::operator::scan::view::SourceViewOperator;

	const RB: RingBufferId = RingBufferId(42);
	const T0: u64 = 1_000_000_000_000;
	const HOUR: u64 = 3_600 * 1_000_000_000;
	const AFTER: u64 = T0 + HOUR + 1_000_000_000;

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
		View::Table(TableView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "rb".to_string(),
			kind: ViewKind::Deferred,
			columns,
			primary_key: None,
			storage: TableId(7),
			sort: vec![],
		})
	}

	fn build_op(partitioned: bool, propagate: bool, ttl: Option<Duration>) -> SinkRingBufferViewOperator {
		let view = view_def(partitioned);
		let resolved = ResolvedView::new(
			Fragment::internal("rb"),
			ResolvedNamespace::new(Fragment::internal("test"), Namespace::system()),
			view.clone(),
		);
		let parent = OperatorCell::new(SourceViewOperator::new(OperatorId(9), view));
		let partition_by = if partitioned {
			vec!["base".to_string()]
		} else {
			Vec::new()
		};
		SinkRingBufferViewOperator::new(parent, OperatorId(1), resolved, RB, 100, propagate, ttl, partition_by)
	}

	fn deferred_txn(engine: &TestEngine) -> FlowTransaction {
		engine.flow_txn().clock_millis(0).deferred()
	}

	fn commit_flow_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		// Mirrors the committer split: state to the arena, everything else to the multi store.
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		for (key, pw) in pending.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::OperatorState)) {
				continue;
			}
			match pw {
				PendingWrite::Set(v) => cmd.set(key, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(key).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(key).unwrap(),
			};
		}
		let version = cmd.commit().unwrap();
		apply_operator_state(&engine.inner().operator_state(), version, &pending);
	}

	fn columns_at(partitioned: bool, rows: &[(&str, i32)], first_source_rn: u64, time: u64) -> Columns {
		let ns: Vec<i32> = rows.iter().map(|(_, n)| *n).collect();
		let rns: Vec<RowNumber> = (0..rows.len() as u64).map(|i| RowNumber(first_source_rn + i)).collect();
		let ts: Vec<DateTime> = rows.iter().map(|_| DateTime::from_nanos(time)).collect();
		let mut cols = Vec::new();
		if partitioned {
			let bases: Vec<String> = rows.iter().map(|(b, _)| b.to_string()).collect();
			cols.push(ColumnWithName::new(Fragment::internal("base"), ColumnBuffer::utf8(bases)));
		}
		cols.push(ColumnWithName::new(Fragment::internal("n"), ColumnBuffer::int4(ns)));
		Columns::with_system(cols, SystemColumns::new(rns, Vec::new(), ts.clone(), ts.clone(), ts))
	}

	fn insert(
		engine: &TestEngine,
		op: &SinkRingBufferViewOperator,
		partitioned: bool,
		rows: &[(&str, i32)],
		first_source_rn: u64,
	) -> CommitVersion {
		insert_at(engine, op, partitioned, rows, first_source_rn, T0)
	}

	fn insert_at(
		engine: &TestEngine,
		op: &SinkRingBufferViewOperator,
		partitioned: bool,
		rows: &[(&str, i32)],
		first_source_rn: u64,
		time: u64,
	) -> CommitVersion {
		let mut txn = deferred_txn(engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				OperatorId(1),
				CommitVersion(1),
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
		op: &SinkRingBufferViewOperator,
		partition_values: &[Value],
		at: u64,
	) -> Option<Change> {
		// Stands in for the dispatcher. Eviction is decided by the timer's own instant, so
		// neither the operator nor the test needs a clock.
		let mut txn = deferred_txn(engine);
		let out = op
			.on_timer(
				&mut txn,
				Timer {
					at: DateTime::from_nanos(at),
					kind: TimerKind::RowTtl,
					key: op.timer_key(partition_values),
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
	fn every_ringbuffer_state_key_is_node_scoped_in_its_own_keyspace() {
		// A hand-rolled leading byte is indistinguishable from a group-id varint, so such a key
		// sits inside whatever group range shares its prefix and can be range-deleted by an
		// unrelated reclaim. Node scope is group 0, which both reclaim phases refuse outright.
		let op = build_op(true, false, None);
		let partition = Partition::of(&[Value::Utf8("sol".to_string())]);

		for (key, expected) in [
			(op.forward_key(RowNumber(42)), Keyspace::RINGBUFFER_FORWARD),
			(op.row_entry_key(Some(partition), RowNumber(42)), Keyspace::RINGBUFFER_ENTRY),
			(op.row_entry_key(None, RowNumber(42)), Keyspace::RINGBUFFER_ENTRY),
		] {
			let (group, keyspace, _) = OperatorGroupStateKey::decode_inner(key.as_bytes())
				.expect("a ringbuffer state key must decode as a structured operator-state key");
			assert_eq!(
				group,
				GroupId::NODE_SCOPE,
				"ringbuffer state must not live inside a reclaimable group"
			);
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
		let prefix =
			OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::RINGBUFFER_FORWARD, vec![]);
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
		txn.get(&key).unwrap().map(|row| decode_ringbuffer_metadata(&row))
	}

	fn base(value: &str) -> Vec<Value> {
		vec![Value::Utf8(value.to_string())]
	}

	#[test]
	fn a_row_ttl_timer_is_a_noop_when_ttl_disabled() {
		// A ttl-less ring arms no RowTtl timer, so this firing can only arrive by mistake:
		// capacity is its only bound, and evicting here truncates a buffer meant to stay whole.
		let engine = TestEngine::new();
		let op = build_op(true, true, None);
		insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);

		let out = fire(&engine, &op, &base("us"), AFTER);
		assert!(out.is_none(), "a ttl-less ring buffer must never evict on a timer");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "no row-entry state may be reclaimed");
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
	}

	#[test]
	fn a_timer_that_fires_before_anything_expires_evicts_nothing() {
		// The direction a cold start lands in: a watermark short of row_time + ttl must find
		// nothing due and never fall back to "evict what looks old".
		let engine = TestEngine::new();
		let op = build_op(true, true, Some(hour_ttl()));
		insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);

		let out = fire(&engine, &op, &base("us"), T0);
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
		let op = build_op(true, true, Some(hour_ttl()));

		insert_at(&engine, &op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &op, true, &[("eu", 3), ("eu", 4)], 3, AFTER);

		let out = fire(&engine, &op, &base("us"), AFTER);
		assert!(out.is_some(), "delete-mode eviction of real rows must announce a downstream change");

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
	fn partial_expiry_decrements_count_and_advances_head_to_the_survivor() {
		let engine = TestEngine::new();
		let op = build_op(true, true, Some(hour_ttl()));

		insert_at(&engine, &op, true, &[("us", 1), ("us", 2)], 1, T0);
		insert_at(&engine, &op, true, &[("us", 3), ("us", 4)], 3, AFTER);

		let before = metadata(&engine, &base("us")).unwrap();
		assert_eq!(before.count, 4);
		let survivor_head = before.head + 2;

		fire(&engine, &op, &base("us"), AFTER);

		let after = metadata(&engine, &base("us")).expect("partition still has survivors");
		assert_eq!(after.count, 2, "the two expired rows must be subtracted");
		assert_eq!(after.head, survivor_head, "head must advance to the oldest surviving row");
		assert_eq!(after.tail, before.tail, "tail must not move on eviction");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "only the two fresh row entries remain");
	}

	#[test]
	fn drop_mode_reclaims_state_but_is_silent() {
		// Suppressing the announcement must not suppress the reclamation: state still goes, only
		// the downstream change is withheld.
		let engine = TestEngine::new();
		let op = build_op(true, false, Some(hour_ttl()));

		insert_at(&engine, &op, true, &[("us", 1), ("us", 2)], 1, T0);

		let out = fire(&engine, &op, &base("us"), AFTER);
		assert!(out.is_none(), "drop mode must not announce evictions downstream");
		assert!(metadata(&engine, &base("us")).is_none(), "drop mode must still reclaim operator state");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 0);
		assert_eq!(forward_count(&engine, &op), 0);
	}

	#[test]
	fn non_partitioned_eviction_reclaims_state() {
		let engine = TestEngine::new();
		let op = build_op(false, true, Some(hour_ttl()));

		insert_at(&engine, &op, false, &[("", 1), ("", 2)], 1, T0);
		insert_at(&engine, &op, false, &[("", 3)], 3, AFTER);

		fire(&engine, &op, &[], AFTER);

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
		let op = build_op(true, true, Some(hour_ttl()));

		insert_at(&engine, &op, true, &[("us", 1)], 1, AFTER);
		let head_before = metadata(&engine, &base("us")).unwrap().head;
		insert_at(&engine, &op, true, &[("us", 2)], 2, T0);

		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2, "precondition: both rows are live");

		fire(&engine, &op, &base("us"), AFTER);

		let after = metadata(&engine, &base("us")).expect("the fresh row survives");
		assert_eq!(after.count, 1, "only the row whose own time already expired is evicted");
		assert_eq!(after.head, head_before, "head stays at the surviving row, the true min survivor");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 1);
	}
}
