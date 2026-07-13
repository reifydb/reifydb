// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
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
		OperatorCapability::STANDARD
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
			let mut verified: HashSet<Partition> = HashSet::new();
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
				resolve_partition_flow(txn, object_id, partition, &values, &mut verified)?;
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
		let mut verified: HashSet<Partition> = HashSet::new();
		for row_idx in 0..row_count {
			let pre_source_rn = source_pre.row_numbers[row_idx];
			let post_source_rn = source_post.row_numbers[row_idx];

			let partition = if self.is_partitioned() {
				let (pre_partition, _) = partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(object_id, pre_partition, post_partition)?;
				resolve_partition_flow(txn, object_id, post_partition, &post_values, &mut verified)?;
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
