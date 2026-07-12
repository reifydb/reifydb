// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use postcard::{from_bytes, to_stdvec};
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
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
		partitioned_row::{PartitionedRowKey, RowLocator},
		ringbuffer::RingBufferMetadataKey,
		row::RowKey,
	},
	row::row_shape_from_columns,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::partition::partition_col_indices;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	value::{
		Value, blob::Blob, datetime::DateTime, partition::Partition, row_number::RowNumber,
		value_type::ValueType,
	},
};
use serde::{Deserialize, Serialize};
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
	operator::{OperatorCell, stateful::raw::RawStatefulOperator},
	transaction::FlowTransaction,
};

const FORWARD_PREFIX: u8 = 0x01;
const ROW_ENTRY_PREFIX: u8 = 0x02;
const MARKER_PREFIX: u8 = 0x03;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RowEntry {
	source_rn: Option<RowNumber>,
	partition: Option<Partition>,
}

pub struct SinkRingBufferViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	view: ResolvedView,
	ringbuffer_id: RingBufferId,
	capacity: u64,
	propagate_evictions: bool,
	state_shape: RowShape,
	partition_indices: Vec<usize>,
}

impl SinkRingBufferViewOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		view: ResolvedView,
		ringbuffer_id: RingBufferId,
		capacity: u64,
		propagate_evictions: bool,
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
			state_shape: RowShape::operator_state(),
			partition_indices,
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
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
			Some(row) => {
				let blob = self.state_shape.get_blob(&row, 0);
				let bytes: [u8; 8] = blob.as_bytes().try_into().map_err(|_| {
					Error::from(FlowStateError::Decode {
						state: "RingBufferForward",
						cause: "expected 8 bytes".to_string(),
					})
				})?;
				Ok(Some(RowNumber(u64::from_be_bytes(bytes))))
			}
			None => Ok(None),
		}
	}

	fn set_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber, storage_rn: RowNumber) -> Result<()> {
		let key = self.forward_key(source_rn);
		let mut row = self.state_shape.allocate();
		self.state_shape.set_blob(&mut row, 0, &Blob::from(storage_rn.0.to_be_bytes().to_vec()));
		self.state_set(txn, &key, row)
	}

	fn remove_forward(&self, txn: &mut FlowTransaction, source_rn: RowNumber) -> Result<()> {
		let key = self.forward_key(source_rn);
		self.state_remove(txn, &key)
	}

	fn row_entry_key(&self, storage_rn: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(9);
		bytes.push(ROW_ENTRY_PREFIX);
		bytes.extend_from_slice(&storage_rn.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	fn get_row_entry(&self, txn: &mut FlowTransaction, storage_rn: RowNumber) -> Result<Option<RowEntry>> {
		let key = self.row_entry_key(storage_rn);
		match self.state_get(txn, &key)? {
			Some(row) => {
				let blob = self.state_shape.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(Some(RowEntry::default()));
				}
				let entry: RowEntry = from_bytes(blob.as_ref()).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "RingBufferRowEntry",
						cause: e.to_string(),
					})
				})?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	fn set_row_entry(&self, txn: &mut FlowTransaction, storage_rn: RowNumber, entry: RowEntry) -> Result<()> {
		let key = self.row_entry_key(storage_rn);
		let serialized = to_stdvec(&entry).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "RingBufferRowEntry",
				cause: e.to_string(),
			})
		})?;
		let mut row = self.state_shape.allocate();
		self.state_shape.set_blob(&mut row, 0, &Blob::from(serialized));
		self.state_set(txn, &key, row)
	}

	fn remove_row_entry(&self, txn: &mut FlowTransaction, storage_rn: RowNumber) -> Result<()> {
		let key = self.row_entry_key(storage_rn);
		self.state_remove(txn, &key)
	}

	fn partition_marker_key(&self, partition: Partition, storage_rn: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(25);
		bytes.push(MARKER_PREFIX);
		bytes.extend_from_slice(&partition.0.to_be_bytes());
		bytes.extend_from_slice(&storage_rn.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	fn set_partition_marker(
		&self,
		txn: &mut FlowTransaction,
		partition: Partition,
		storage_rn: RowNumber,
	) -> Result<()> {
		let key = self.partition_marker_key(partition, storage_rn);
		let row = self.state_shape.allocate();
		self.state_set(txn, &key, row)
	}

	fn remove_partition_marker(
		&self,
		txn: &mut FlowTransaction,
		partition: Partition,
		storage_rn: RowNumber,
	) -> Result<()> {
		let key = self.partition_marker_key(partition, storage_rn);
		self.state_remove(txn, &key)
	}

	fn oldest_row_in_partition(
		&self,
		txn: &mut FlowTransaction,
		partition: Partition,
	) -> Result<Option<RowNumber>> {
		let mut prefix = Vec::with_capacity(17);
		prefix.push(MARKER_PREFIX);
		prefix.extend_from_slice(&partition.0.to_be_bytes());
		let range = EncodedKeyRange::prefix(&prefix);
		if let Some(entry) = self.state_range(txn, range).next() {
			let (key, _) = entry?;
			return Ok(row_number_from_marker_key(key.as_slice()));
		}
		Ok(None)
	}
}

impl RawStatefulOperator for SinkRingBufferViewOperator {}

impl Operator for SinkRingBufferViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let shape = row_shape_from_columns(view.columns());
		let object_id = ShapeId::ringbuffer(self.ringbuffer_id);
		let mut metadata = self.read_metadata(txn)?;
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
				} => self.apply_ringbuffer_remove(txn, &view, object_id, pre)?,
			}
		}

		self.write_metadata(txn, &metadata)?;
		for (partition_values, partition_meta) in partition_metadata.iter() {
			if partition_meta.is_empty() {
				self.remove_partition_metadata(txn, partition_values)?;
			} else {
				self.write_partition_metadata(txn, partition_values, partition_meta)?;
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkRingBufferViewOperator {
	#[inline]
	#[allow(clippy::too_many_arguments)]
	fn apply_ringbuffer_insert(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		object_id: ShapeId,
		metadata: &mut RingBufferMetadata,
		partition_metadata: &mut HashMap<Vec<Value>, RingBufferMetadata>,
		post: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, shape);
		let mut assigned_ids: Vec<RowNumber> = Vec::with_capacity(row_count);
		let mut assigned_partitions: Vec<Option<Partition>> = Vec::with_capacity(row_count);
		let mut encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		let mut evicted_in_batch: HashSet<RowNumber> = HashSet::new();
		let mut verified: HashSet<Partition> = HashSet::new();

		let mut batch_assignments: HashMap<RowNumber, usize> = HashMap::new();
		let mut evicted_rns: Vec<RowNumber> = Vec::new();
		let mut evicted_rows: Vec<EncodedRow> = Vec::new();
		for row_idx in 0..row_count {
			let partition_info =
				self.is_partitioned().then(|| partition_of(&self.partition_indices, &coerced, row_idx));

			if let Some((partition, values)) = &partition_info {
				if !partition_metadata.contains_key(values) {
					let loaded = self.read_partition_metadata(txn, values)?;
					partition_metadata.insert(values.clone(), loaded);
				}
				let pm = partition_metadata.get_mut(values).unwrap();
				if pm.is_full()
					&& let Some(oldest_rn) = self.oldest_row_in_partition(txn, *partition)?
				{
					self.remove_partition_marker(txn, *partition, oldest_rn)?;
					let entry = self.get_row_entry(txn, oldest_rn)?;
					let source_rn = entry.as_ref().and_then(|e| e.source_rn);
					if let Some(source_rn) = source_rn {
						self.remove_forward(txn, source_rn)?;
					}
					self.remove_row_entry(txn, oldest_rn)?;

					let pre_key = self.rb_key(object_id, oldest_rn, Some(*partition));
					if self.propagate_evictions {
						let evicted = match batch_assignments.get(&oldest_rn) {
							Some(&idx) => Some((
								source.row_numbers[idx],
								encoded_rows[idx].clone(),
							)),
							None => txn
								.get(&pre_key)?
								.map(|row| (source_rn.unwrap_or(oldest_rn), row)),
						};
						if let Some((rn, row)) = evicted {
							evicted_rns.push(rn);
							evicted_rows.push(row);
						}
					}
					txn.remove(&pre_key)?;
					evicted_in_batch.insert(oldest_rn);
					pm.count -= 1;
				}
			} else if metadata.is_full() {
				let oldest_rn = RowNumber(metadata.head);
				let entry = self.get_row_entry(txn, oldest_rn)?;
				let source_rn = entry.as_ref().and_then(|e| e.source_rn);
				if let Some(source_rn) = source_rn {
					self.remove_forward(txn, source_rn)?;
				}
				self.remove_row_entry(txn, oldest_rn)?;

				let pre_key = self.rb_key(object_id, oldest_rn, None);
				if self.propagate_evictions {
					let evicted = match batch_assignments.get(&oldest_rn) {
						Some(&idx) => {
							Some((source.row_numbers[idx], encoded_rows[idx].clone()))
						}
						None => txn
							.get(&pre_key)?
							.map(|row| (source_rn.unwrap_or(oldest_rn), row)),
					};
					if let Some((rn, row)) = evicted {
						evicted_rns.push(rn);
						evicted_rows.push(row);
					}
				}
				txn.remove(&pre_key)?;
				metadata.head += 1;
				metadata.count -= 1;
				evicted_in_batch.insert(oldest_rn);
			}

			let source_rn = source.row_numbers[row_idx];
			let assigned_rn = RowNumber(metadata.tail);
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, assigned_rn, &field_columns)?;

			if source_rn != assigned_rn {
				self.set_forward(txn, source_rn, assigned_rn)?;
			}

			let row_partition = partition_info.as_ref().map(|(p, _)| *p);
			if source_rn != assigned_rn || row_partition.is_some() {
				self.set_row_entry(
					txn,
					assigned_rn,
					RowEntry {
						source_rn: (source_rn != assigned_rn).then_some(source_rn),
						partition: row_partition,
					},
				)?;
			}

			if let Some((partition, values)) = &partition_info {
				resolve_partition_flow(txn, object_id, *partition, values, &mut verified)?;
				self.set_partition_marker(txn, *partition, assigned_rn)?;
				let pm = partition_metadata.get_mut(values).unwrap();
				if pm.is_empty() {
					pm.head = assigned_rn.0;
				}
				pm.count += 1;
				pm.tail = assigned_rn.0 + 1;
			}

			assigned_ids.push(assigned_rn);
			assigned_partitions.push(row_partition);
			encoded_rows.push(encoded);
			if self.propagate_evictions {
				batch_assignments.insert(assigned_rn, row_idx);
			}

			if metadata.is_empty() {
				metadata.head = assigned_rn.0;
			}
			metadata.count += 1;
			metadata.tail = assigned_rn.0 + 1;
		}

		let surviving: Vec<usize> =
			(0..assigned_ids.len()).filter(|&i| !evicted_in_batch.contains(&assigned_ids[i])).collect();
		let final_ids: Vec<RowNumber> = surviving.iter().map(|&i| assigned_ids[i]).collect();
		let final_partitions: Vec<Option<Partition>> =
			surviving.iter().map(|&i| assigned_partitions[i]).collect();
		let final_rows: Vec<EncodedRow> = surviving.iter().map(|&i| encoded_rows[i].clone()).collect();

		for ((assigned_rn, partition), encoded) in
			final_ids.iter().zip(final_partitions.iter()).zip(final_rows.iter())
		{
			let key = self.rb_key(object_id, *assigned_rn, *partition);
			txn.set(&key, encoded.clone())?;
		}
		emit_view_change(txn, view, Diff::insert(coerced));

		if let Some(diff) = self.build_evicted_diff(txn, view, shape, evicted_rns, evicted_rows)? {
			emit_view_change(txn, view, diff);
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
	#[allow(clippy::too_many_arguments)]
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
		let mut pre_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut post_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		let mut verified: HashSet<Partition> = HashSet::new();
		for row_idx in 0..row_count {
			let pre_source_rn = source_pre.row_numbers[row_idx];
			let post_source_rn = source_post.row_numbers[row_idx];
			let pre_storage_rn = self.get_forward(txn, pre_source_rn)?.unwrap_or(pre_source_rn);
			let post_storage_rn = self.get_forward(txn, post_source_rn)?.unwrap_or(post_source_rn);
			let (_, post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_storage_rn, &field_columns)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(object_id, pre_partition, post_partition)?;
				resolve_partition_flow(txn, object_id, post_partition, &post_values, &mut verified)?;

				let storage_rn_changed = pre_storage_rn != post_storage_rn;
				let partition_changed = pre_partition != post_partition;

				if storage_rn_changed || partition_changed {
					self.remove_partition_marker(txn, pre_partition, pre_storage_rn)?;
					if pre_source_rn != pre_storage_rn {
						self.remove_forward(txn, pre_source_rn)?;
					}
					self.remove_row_entry(txn, pre_storage_rn)?;
				}

				if partition_changed {
					let mut pre_pm = self.read_partition_metadata(txn, &pre_values)?;
					pre_pm.count = pre_pm.count.saturating_sub(1);
					if pre_pm.is_empty() {
						self.remove_partition_metadata(txn, &pre_values)?;
					} else {
						self.write_partition_metadata(txn, &pre_values, &pre_pm)?;
					}

					let mut post_pm = self.read_partition_metadata(txn, &post_values)?;
					if post_pm.is_empty() {
						post_pm.head = post_storage_rn.0;
					}
					post_pm.count += 1;
					post_pm.tail = post_storage_rn.0 + 1;
					self.write_partition_metadata(txn, &post_values, &post_pm)?;
				}

				self.set_row_entry(
					txn,
					post_storage_rn,
					RowEntry {
						source_rn: (post_source_rn != post_storage_rn)
							.then_some(post_source_rn),
						partition: Some(post_partition),
					},
				)?;
				self.set_partition_marker(txn, post_partition, post_storage_rn)?;
				if post_source_rn != post_storage_rn {
					self.set_forward(txn, post_source_rn, post_storage_rn)?;
				}

				(
					self.rb_key(object_id, pre_storage_rn, Some(pre_partition)),
					self.rb_key(object_id, post_storage_rn, Some(post_partition)),
				)
			} else {
				if pre_storage_rn != post_storage_rn {
					if pre_source_rn != pre_storage_rn {
						self.remove_forward(txn, pre_source_rn)?;
					}
					self.remove_row_entry(txn, pre_storage_rn)?;
				}
				if post_source_rn != post_storage_rn {
					self.set_forward(txn, post_source_rn, post_storage_rn)?;
					self.set_row_entry(
						txn,
						post_storage_rn,
						RowEntry {
							source_rn: Some(post_source_rn),
							partition: None,
						},
					)?;
				}
				(
					RowKey::encoded(object_id, pre_storage_rn),
					RowKey::encoded(object_id, post_storage_rn),
				)
			};
			pre_keys.push(pre_key);
			post_keys.push(post_key);
			post_encoded_rows.push(post_encoded);
		}

		for ((pre_key, post_key), post_encoded) in
			pre_keys.iter().zip(post_keys.iter()).zip(post_encoded_rows.iter())
		{
			txn.remove(pre_key)?;
			txn.set(post_key, post_encoded.clone())?;
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
		pre: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let row_count = coerced.row_count();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_rn = coerced.row_numbers[row_idx];
			let storage_rn = self.get_forward(txn, source_rn)?.unwrap_or(source_rn);
			if source_rn != storage_rn {
				self.remove_forward(txn, source_rn)?;
			}

			let entry = self.get_row_entry(txn, storage_rn)?;
			let partition = entry.as_ref().and_then(|e| e.partition);
			if let Some(partition) = partition {
				self.remove_partition_marker(txn, partition, storage_rn)?;
				let (_, partition_values) = partition_of(&self.partition_indices, &coerced, row_idx);
				let mut pm = self.read_partition_metadata(txn, &partition_values)?;
				pm.count = pm.count.saturating_sub(1);
				if pm.is_empty() {
					self.remove_partition_metadata(txn, &partition_values)?;
				} else {
					self.write_partition_metadata(txn, &partition_values, &pm)?;
				}
			}
			if entry.is_some() {
				self.remove_row_entry(txn, storage_rn)?;
			}

			keys.push(self.rb_key(object_id, storage_rn, partition));
		}
		for key in keys.iter() {
			txn.remove(key)?;
		}
		emit_view_change(txn, view, Diff::remove(coerced));
		Ok(())
	}
}

fn row_number_from_marker_key(bytes: &[u8]) -> Option<RowNumber> {
	if bytes.len() < 8 {
		return None;
	}
	let suffix: [u8; 8] = bytes[bytes.len() - 8..].try_into().ok()?;
	Some(RowNumber(u64::from_be_bytes(suffix)))
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
