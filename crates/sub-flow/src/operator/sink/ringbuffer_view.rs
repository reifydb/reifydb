// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cell::{RefCell, UnsafeCell},
	collections::HashMap,
	ops::Bound,
};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			flow::FlowNodeId,
			id::RingBufferId,
			ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata, encode_ringbuffer_metadata},
			shape::ShapeId,
			view::View,
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		ringbuffer::RingBufferMetadataKey,
		row::RowKey,
	},
	row::row_shape_from_columns,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::partition::partition_col_indices;
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_sdk::operator::Tick;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{
		Value, blob::Blob, datetime::DateTime, duration::Duration, partition::Partition, row_number::RowNumber,
		value_type::ValueType,
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
	Operator,
	error::FlowStateError,
	operator::{
		OperatorCell,
		stateful::{raw::RawStatefulOperator, utils::state_range_versioned},
	},
	transaction::FlowTransaction,
};

const FORWARD_PREFIX: u8 = 0x01;
const ROW_ENTRY_PREFIX: u8 = 0x02;

pub struct SinkRingBufferViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	view: ResolvedView,
	ringbuffer_id: RingBufferId,
	capacity: u64,
	propagate_evictions: bool,
	ttl_nanos: Option<u64>,
	version_epoch: VersionEpoch,
	evict_cursor: RefCell<Option<EncodedKey>>,
	state_shape: RowShape,
	partition_indices: Vec<usize>,
	verified_partitions: UnsafeCell<HashMap<Partition, Vec<Value>>>,
}

impl SinkRingBufferViewOperator {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		view: ResolvedView,
		ringbuffer_id: RingBufferId,
		capacity: u64,
		propagate_evictions: bool,
		ttl_nanos: Option<u64>,
		version_epoch: VersionEpoch,
		partition_by: Vec<String>,
	) -> Self {
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		Self {
			parent,
			node,
			view,
			ringbuffer_id,
			capacity,
			propagate_evictions,
			ttl_nanos,
			version_epoch,
			evict_cursor: RefCell::new(None),
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
		unsafe { &mut *self.verified_partitions.get() }
	}

	#[inline]
	fn rb_key(&self, object_id: ShapeId, rn: RowNumber, partition: Option<Partition>) -> EncodedKey {
		match partition {
			Some(partition) => PartitionedRowKey::encoded(object_id, partition, RowLocator::Row(rn)),
			None => RowKey::encoded(object_id, rn),
		}
	}

	fn read_metadata(&self, txn: &mut FlowTransaction) -> Result<RingBufferMetadata> {
		let key = RingBufferMetadataKey::encoded(self.ringbuffer_id);
		match txn.get(&key)? {
			Some(row) => Ok(decode_ringbuffer_metadata(&row)),
			None => Ok(RingBufferMetadata::new(self.ringbuffer_id, self.capacity)),
		}
	}

	fn write_metadata(&self, txn: &mut FlowTransaction, metadata: &RingBufferMetadata) -> Result<()> {
		let key = RingBufferMetadataKey::encoded(self.ringbuffer_id);
		let row = encode_ringbuffer_metadata(metadata);
		txn.set(&key, row)
	}

	fn read_partition_metadata(
		&self,
		txn: &mut FlowTransaction,
		partition_values: &[Value],
	) -> Result<RingBufferMetadata> {
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		match txn.get(&key)? {
			Some(row) => Ok(decode_ringbuffer_metadata(&row)),
			None => Ok(RingBufferMetadata::new(self.ringbuffer_id, self.capacity)),
		}
	}

	fn write_partition_metadata(
		&self,
		txn: &mut FlowTransaction,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		let row = encode_ringbuffer_metadata(metadata);
		txn.set(&key, row)
	}

	fn remove_partition_metadata(&self, txn: &mut FlowTransaction, partition_values: &[Value]) -> Result<()> {
		let key = RingBufferMetadataKey::encoded_partition(self.ringbuffer_id, partition_values.to_vec());
		txn.remove(&key)
	}

	fn forward_key(&self, source_rn: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(9);
		bytes.push(FORWARD_PREFIX);
		bytes.extend_from_slice(&source_rn.0.to_be_bytes());
		EncodedKey::new(bytes)
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
		self.state_set(txn, &key, row)
	}

	fn drop_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber) -> Result<()> {
		let key = self.forward_key(source_rn);
		self.state_drop(txn, &key)
	}

	fn row_entry_key(&self, partition: Option<Partition>, storage_rn: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(25);
		bytes.push(ROW_ENTRY_PREFIX);
		if let Some(partition) = partition {
			bytes.extend_from_slice(&partition.0.to_be_bytes());
		}
		bytes.extend_from_slice(&storage_rn.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	fn set_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
		source_rn: RowNumber,
	) -> Result<()> {
		let key = self.row_entry_key(partition, storage_rn);
		let mut row = self.state_shape.allocate();
		self.state_shape.set_blob(&mut row, 0, &Blob::from(source_rn.0.to_be_bytes().to_vec()));
		self.state_set(txn, &key, row)
	}

	fn drop_row_entry(
		&self,
		txn: &mut FlowTransaction,
		partition: Option<Partition>,
		storage_rn: RowNumber,
	) -> Result<()> {
		let key = self.row_entry_key(partition, storage_rn);
		self.state_drop(txn, &key)
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
				let source_rn = self.decode_row_number(&row, "RingBufferRowEntry")?;
				self.drop_forward(txn, source_rn)?;
				self.state_drop(txn, &key)?;
				Ok(Some(source_rn))
			}
			None => Ok(None),
		}
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
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_TICK
	}

	fn ticks(&self) -> Option<Duration> {
		if self.ttl_nanos.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape = row_shape_from_columns(view.columns());
		let object_id = ShapeId::ringbuffer(self.ringbuffer_id);
		let mut metadata = if self.is_partitioned() {
			None
		} else {
			Some(self.read_metadata(txn)?)
		};
		let mut partition_metadata: HashMap<Vec<Value>, RingBufferMetadata> = HashMap::new();

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
				)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_ringbuffer_update(txn, &view, &shape, object_id, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_ringbuffer_remove(txn, &view, object_id, &mut metadata, pre)?,
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

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};
		let now_nanos = tick.now.to_nanos();
		let Some(cutoff_nanos) = now_nanos.checked_sub(ttl_nanos) else {
			return Ok(None);
		};
		let Some(cutoff_version) = self.version_epoch.floor_version_at(cutoff_nanos).map(CommitVersion) else {
			return Ok(None);
		};

		const PARTITION_BATCH: usize = 256;
		let base = RingBufferMetadataKey::full_scan_for_ringbuffer(self.ringbuffer_id);
		let start = match self.evict_cursor.borrow().clone() {
			Some(cursor) => Bound::Excluded(cursor),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let partitions = txn
			.range(range, RangeScope::All, 1024)
			.take(PARTITION_BATCH)
			.map(|result| {
				let multi = result?;
				let decoded = RingBufferMetadataKey::decode(&multi.key).ok_or_else(|| {
					Error::from(FlowStateError::Decode {
						state: "RingBufferMetadataKey",
						cause: "malformed partition metadata key".to_string(),
					})
				})?;
				Ok((multi.key, decoded.partition_values))
			})
			.collect::<Result<Vec<(EncodedKey, Vec<Value>)>>>()?;
		let reached_end = partitions.len() < PARTITION_BATCH;
		let last_key = partitions.last().map(|(key, _)| key.clone());

		let view = self.view.def().clone();
		let shape = row_shape_from_columns(view.columns());
		let object_id = ShapeId::ringbuffer(self.ringbuffer_id);
		let mut evicted_rns: Vec<RowNumber> = Vec::new();
		let mut evicted_rows: Vec<EncodedRow> = Vec::new();

		for (_key, partition_values) in partitions {
			self.evict_partition_expired(
				txn,
				object_id,
				&partition_values,
				cutoff_version,
				&mut evicted_rns,
				&mut evicted_rows,
			)?;
		}

		*self.evict_cursor.borrow_mut() = if reached_end {
			None
		} else {
			last_key
		};

		if let Some(diff) = self.build_evicted_diff(txn, &view, &shape, evicted_rns, evicted_rows)? {
			emit_view_change(txn, &view, diff);
			let version = txn.version();
			let changed_at = DateTime::from_nanos(txn.clock().now_nanos());
			return Ok(Some(Change::from_flow(self.node, version, Vec::new(), changed_at)));
		}
		Ok(None)
	}
}

impl SinkRingBufferViewOperator {
	fn evict_partition_expired(
		&self,
		txn: &mut FlowTransaction,
		object_id: ShapeId,
		partition_values: &[Value],
		cutoff_version: CommitVersion,
		evicted_rns: &mut Vec<RowNumber>,
		evicted_rows: &mut Vec<EncodedRow>,
	) -> Result<()> {
		let partition = if partition_values.is_empty() {
			None
		} else {
			Some(Partition::of(partition_values))
		};

		let mut prefix = Vec::with_capacity(9);
		prefix.push(ROW_ENTRY_PREFIX);
		if let Some(partition) = partition {
			prefix.extend_from_slice(&partition.0.to_be_bytes());
		}
		let range = EncodedKeyRange::prefix(&prefix);
		let entries = state_range_versioned(self.node, txn, range)
			.map(|result| {
				let (key, version, _row) = result?;
				let bytes = key.as_ref();
				let rn_bytes: [u8; 8] = bytes[bytes.len() - 8..].try_into().map_err(|_| {
					Error::from(FlowStateError::Decode {
						state: "RingBufferRowEntry",
						cause: "row-entry key shorter than 8 bytes".to_string(),
					})
				})?;
				Ok((u64::from_be_bytes(rn_bytes), version))
			})
			.collect::<Result<Vec<(u64, CommitVersion)>>>()?;

		if entries.is_empty() {
			return Ok(());
		}

		let mut to_evict: Vec<u64> = Vec::new();
		let mut new_head: Option<u64> = None;
		for (storage_rn, version) in entries {
			if version <= cutoff_version {
				to_evict.push(storage_rn);
			} else if new_head.is_none() {
				new_head = Some(storage_rn);
			}
		}

		if to_evict.is_empty() {
			return Ok(());
		}

		let evicted_count = to_evict.len() as u64;
		for storage_rn in to_evict {
			let rn = RowNumber(storage_rn);
			let pre_key = self.rb_key(object_id, rn, partition);
			let row = txn.get(&pre_key)?;
			let source_rn = self.take_row_entry(txn, partition, rn)?;
			if self.propagate_evictions
				&& let Some(row) = row
			{
				evicted_rns.push(source_rn.unwrap_or(rn));
				evicted_rows.push(row);
			}
			txn.drop_key(&pre_key)?;
		}

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
		object_id: ShapeId,
		metadata: &mut Option<RingBufferMetadata>,
		partition_metadata: &mut HashMap<Vec<Value>, RingBufferMetadata>,
		post: &Columns,
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
				resolve_partition_flow(txn, object_id, partition, &values, verified)?;
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
		object_id: ShapeId,
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
			if self.propagate_evictions {
				evicted_rns.push(source_rn.unwrap_or(oldest_rn));
				evicted_rows.push(row);
			}
			txn.drop_key(&pre_key)?;
			meta.count = meta.count.saturating_sub(1);
			evict_needed -= 1;
		}

		let skip = evict_needed.min(incoming) as usize;
		for &row_idx in &rows[..skip] {
			meta.tail += 1;
			if self.propagate_evictions {
				let source_rn = source.row_numbers[row_idx];
				let (_, encoded) =
					encode_row_at_index(source, row_idx, shape, source_rn, field_columns)?;
				evicted_rns.push(source_rn);
				evicted_rows.push(encoded);
			}
		}

		for &row_idx in &rows[skip..] {
			let source_rn = source.row_numbers[row_idx];
			let assigned_rn = RowNumber(meta.tail);
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, assigned_rn, field_columns)?;
			self.set_forward(txn, source_rn, assigned_rn)?;
			self.set_row_entry(txn, partition, assigned_rn, source_rn)?;
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
		if !self.propagate_evictions || evicted_rows.is_empty() {
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
		let mut evicted = Columns::with_system_columns(storage_columns, Vec::new(), Vec::new(), Vec::new());
		evicted.append_rows(shape, evicted_rows, evicted_rns)?;
		decode_dictionary_columns(&mut evicted, txn)?;
		Ok(Some(Diff::remove(evicted)))
	}

	#[inline]
	fn apply_ringbuffer_update(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: ShapeId,
		pre: &Columns,
		post: &Columns,
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
		for row_idx in 0..row_count {
			let pre_source_rn = source_pre.row_numbers[row_idx];
			let post_source_rn = source_post.row_numbers[row_idx];

			let partition = if self.is_partitioned() {
				let (pre_partition, _) = partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(object_id, pre_partition, post_partition)?;
				resolve_partition_flow(txn, object_id, post_partition, &post_values, verified)?;
				Some(post_partition)
			} else {
				None
			};

			let Some(storage_rn) = self.get_forward(txn, pre_source_rn)? else {
				continue;
			};
			let key = self.rb_key(object_id, storage_rn, partition);

			if post_source_rn != pre_source_rn {
				self.drop_forward(txn, pre_source_rn)?;
				self.set_forward(txn, post_source_rn, storage_rn)?;
				self.set_row_entry(txn, partition, storage_rn, post_source_rn)?;
			}

			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, storage_rn, &field_columns)?;
			txn.set(&key, post_encoded)?;
		}
		emit_view_change(txn, view, Diff::update(coerced_pre, coerced_post));
		Ok(())
	}

	#[inline]
	fn apply_ringbuffer_remove(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		object_id: ShapeId,
		metadata: &mut Option<RingBufferMetadata>,
		pre: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		for row_idx in 0..row_count {
			let source_rn = coerced.row_numbers[row_idx];
			let Some(storage_rn) = self.get_forward(txn, source_rn)? else {
				continue;
			};

			let (partition, partition_values) = if self.is_partitioned() {
				let (partition, partition_values) =
					partition_of(&self.partition_indices, &coerced, row_idx);
				(Some(partition), Some(partition_values))
			} else {
				(None, None)
			};

			self.drop_forward(txn, source_rn)?;
			self.drop_row_entry(txn, partition, storage_rn)?;

			let key = self.rb_key(object_id, storage_rn, partition);
			txn.remove(&key)?;

			if let Some(partition_values) = partition_values {
				let mut pm = self.read_partition_metadata(txn, &partition_values)?;
				pm.count = pm.count.saturating_sub(1);
				if pm.is_empty() {
					self.remove_partition_metadata(txn, &partition_values)?;
				} else {
					self.write_partition_metadata(txn, &partition_values, &pm)?;
				}
			} else {
				let meta = metadata
					.as_mut()
					.expect("non-partitioned ring buffer sink must have loaded global metadata");
				meta.count = meta.count.saturating_sub(1);
			}
		}
		emit_view_change(txn, view, Diff::remove(coerced));
		Ok(())
	}
}

#[inline]
fn emit_view_change(txn: &mut FlowTransaction, view: &View, diff: Diff) {
	let version = txn.version();
	let changed_at = DateTime::from_nanos(txn.clock().now_nanos());
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Shape(ShapeId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		actors::pending::PendingWrite,
		interface::{
			catalog::{
				column::{Column as CatalogColumn, ColumnIndex},
				id::{ColumnId, NamespaceId, TableId, ViewId},
				namespace::Namespace,
				view::{TableView, ViewKind},
			},
			resolved::ResolvedNamespace,
		},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{constraint::TypeConstraint, identity::IdentityId};

	use super::*;
	use crate::operator::{Operators, scan::view::PrimitiveViewOperator};

	const RB: RingBufferId = RingBufferId(42);
	const T0: u64 = 1_000_000_000_000;
	const HOUR: u64 = 3_600 * 1_000_000_000;
	const AFTER: u64 = T0 + HOUR + 1_000_000_000;

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
			underlying: TableId(7),
			sort: vec![],
		})
	}

	fn build_op(partitioned: bool, propagate: bool, ttl_nanos: Option<u64>) -> SinkRingBufferViewOperator {
		let view = view_def(partitioned);
		let resolved = ResolvedView::new(
			Fragment::internal("rb"),
			ResolvedNamespace::new(Fragment::internal("test"), Namespace::system()),
			view.clone(),
		);
		let parent = OperatorCell::new(Operators::SourceView(PrimitiveViewOperator::new(FlowNodeId(9), view)));
		let partition_by = if partitioned {
			vec!["base".to_string()]
		} else {
			Vec::new()
		};
		SinkRingBufferViewOperator::new(
			parent,
			FlowNodeId(1),
			resolved,
			RB,
			100,
			propagate,
			ttl_nanos,
			VersionEpoch::new(),
			partition_by,
		)
	}

	fn deferred_txn(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	fn commit_flow_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		for (key, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(key, v.clone()).unwrap(),
				PendingWrite::Remove => cmd.remove(key).unwrap(),
				PendingWrite::Drop => cmd.drop_key(key).unwrap(),
			};
		}
		cmd.commit().unwrap();
	}

	fn columns(partitioned: bool, rows: &[(&str, i32)], first_source_rn: u64) -> Columns {
		let ns: Vec<i32> = rows.iter().map(|(_, n)| *n).collect();
		let rns: Vec<RowNumber> = (0..rows.len() as u64).map(|i| RowNumber(first_source_rn + i)).collect();
		let ts: Vec<DateTime> = rows.iter().map(|_| DateTime::from_nanos(T0)).collect();
		let mut cols = Vec::new();
		if partitioned {
			let bases: Vec<String> = rows.iter().map(|(b, _)| b.to_string()).collect();
			cols.push(ColumnWithName::new(Fragment::internal("base"), ColumnBuffer::utf8(bases)));
		}
		cols.push(ColumnWithName::new(Fragment::internal("n"), ColumnBuffer::int4(ns)));
		Columns::with_system_columns(cols, rns, ts.clone(), ts)
	}

	fn insert(
		engine: &TestEngine,
		op: &SinkRingBufferViewOperator,
		partitioned: bool,
		rows: &[(&str, i32)],
		first_source_rn: u64,
	) -> CommitVersion {
		let mut txn = deferred_txn(engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				FlowNodeId(1),
				CommitVersion(1),
				vec![Diff::insert(columns(partitioned, rows, first_source_rn))],
				DateTime::from_nanos(T0),
			),
		)
		.unwrap();
		commit_flow_pending(engine, &mut txn);
		engine.current_version().unwrap()
	}

	fn tick(engine: &TestEngine, op: &SinkRingBufferViewOperator, now: u64) -> Option<Change> {
		let mut txn = deferred_txn(engine);
		let out = op
			.tick(
				&mut txn,
				Tick {
					now: DateTime::from_nanos(now),
				},
			)
			.unwrap();
		commit_flow_pending(engine, &mut txn);
		out
	}

	fn partition_prefix(values: &[Value]) -> Vec<u8> {
		let mut prefix = vec![ROW_ENTRY_PREFIX];
		if !values.is_empty() {
			prefix.extend_from_slice(&Partition::of(values).0.to_be_bytes());
		}
		prefix
	}

	fn row_entry_count(engine: &TestEngine, op: &SinkRingBufferViewOperator, values: &[Value]) -> usize {
		let mut txn = deferred_txn(engine);
		let prefix = partition_prefix(values);
		op.state_range(&mut txn, EncodedKeyRange::prefix(&prefix)).collect::<Result<Vec<_>>>().unwrap().len()
	}

	fn forward_count(engine: &TestEngine, op: &SinkRingBufferViewOperator) -> usize {
		let mut txn = deferred_txn(engine);
		op.state_range(&mut txn, EncodedKeyRange::prefix(&[FORWARD_PREFIX]))
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
	fn tick_is_noop_when_ttl_disabled() {
		let engine = TestEngine::new();
		let op = build_op(true, true, None);
		insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);

		let out = tick(&engine, &op, AFTER);
		assert!(out.is_none(), "a ttl-less ring buffer must never evict on tick");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "no row-entry state may be reclaimed");
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
	}

	#[test]
	fn tick_is_conservative_when_epoch_has_no_sample() {
		let engine = TestEngine::new();
		let op = build_op(true, true, Some(HOUR));
		insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);

		let out = tick(&engine, &op, AFTER);
		assert!(out.is_none());
		assert_eq!(
			row_entry_count(&engine, &op, &base("us")),
			2,
			"with no epoch sample the cutoff is None and nothing may be evicted"
		);
		assert_eq!(metadata(&engine, &base("us")).unwrap().count, 2);
	}

	#[test]
	fn expired_partition_state_is_fully_reclaimed_and_active_partition_survives() {
		// The leak this fix exists for: a quiet partition's per-partition operator state
		// (forward map, row entries, metadata key) must be reclaimed, not stranded. A partition
		// that received fresher rows must be left entirely untouched.
		let engine = TestEngine::new();
		let op = build_op(true, true, Some(HOUR));

		let v_old = insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);
		op.version_epoch.record(T0, v_old.0);
		insert(&engine, &op, true, &[("eu", 3), ("eu", 4)], 3);

		let out = tick(&engine, &op, AFTER);
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
		let op = build_op(true, true, Some(HOUR));

		let v_old = insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);
		op.version_epoch.record(T0, v_old.0);
		insert(&engine, &op, true, &[("us", 3), ("us", 4)], 3);

		let before = metadata(&engine, &base("us")).unwrap();
		assert_eq!(before.count, 4);
		let survivor_head = before.head + 2;

		tick(&engine, &op, AFTER);

		let after = metadata(&engine, &base("us")).expect("partition still has survivors");
		assert_eq!(after.count, 2, "the two expired rows must be subtracted");
		assert_eq!(after.head, survivor_head, "head must advance to the oldest surviving row");
		assert_eq!(after.tail, before.tail, "tail must not move on eviction");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 2, "only the two fresh row entries remain");
	}

	#[test]
	fn drop_mode_reclaims_state_but_is_silent() {
		// cleanup_mode: drop => propagate_evictions false. State must STILL be reclaimed (the
		// leak fix), but no downstream change may be announced.
		let engine = TestEngine::new();
		let op = build_op(true, false, Some(HOUR));

		let v_old = insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);
		op.version_epoch.record(T0, v_old.0);

		let out = tick(&engine, &op, AFTER);
		assert!(out.is_none(), "drop mode must not announce evictions downstream");
		assert!(metadata(&engine, &base("us")).is_none(), "drop mode must still reclaim operator state");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 0);
		assert_eq!(forward_count(&engine, &op), 0);
	}

	#[test]
	fn non_partitioned_eviction_reclaims_state() {
		let engine = TestEngine::new();
		let op = build_op(false, true, Some(HOUR));

		let v_old = insert(&engine, &op, false, &[("", 1), ("", 2)], 1);
		op.version_epoch.record(T0, v_old.0);
		insert(&engine, &op, false, &[("", 3)], 3);

		tick(&engine, &op, AFTER);

		let global = metadata(&engine, &[]).expect("the global ring keeps a single metadata key");
		assert_eq!(global.count, 1, "only the fresh row remains counted");
		assert_eq!(row_entry_count(&engine, &op, &[]), 1, "only the fresh row entry remains");
		assert_eq!(forward_count(&engine, &op), 1);
	}

	#[test]
	fn min_survivor_head_is_correct_when_a_refreshed_row_outlives_an_older_neighbour() {
		// An update that changes a row's source row number rewrites its row entry, giving it a
		// newer commit version than a physically-later neighbour. Eviction must then key off the
		// actual per-row version (min survivor), not assume expired rows form a head prefix.
		let engine = TestEngine::new();
		let op = build_op(true, true, Some(HOUR));

		let v_old = insert(&engine, &op, true, &[("us", 1), ("us", 2)], 1);
		op.version_epoch.record(T0, v_old.0);

		let head_before = metadata(&engine, &base("us")).unwrap().head;

		let mut txn = deferred_txn(&engine);
		op.apply(
			&mut txn,
			Change::from_flow(
				FlowNodeId(1),
				CommitVersion(1),
				vec![Diff::update(columns(true, &[("us", 1)], 1), columns(true, &[("us", 1)], 9))],
				DateTime::from_nanos(T0),
			),
		)
		.unwrap();
		commit_flow_pending(&engine, &mut txn);

		tick(&engine, &op, AFTER);

		let after = metadata(&engine, &base("us")).expect("the refreshed row survives");
		assert_eq!(after.count, 1, "only the un-refreshed older neighbour is evicted");
		assert_eq!(after.head, head_before, "head stays at the refreshed row, the true min survivor");
		assert_eq!(row_entry_count(&engine, &op, &base("us")), 1);
	}
}
