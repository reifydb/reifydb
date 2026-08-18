// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::UnsafeCell, collections::HashMap};

use reifydb_core::interface::flow::OperatorCapability;
use reifydb_codec::row::shape::RowFamily;
use reifydb_codec::{
	row::{
		bytes::SHAPE_HEADER_SIZE,
		bytes::{EncodedBytes, RowBuilder},
		table::EncodedTableRow,
		operator::{read_created_at, read_updated_at},
		shape::RowShape,
	},
	key::{encode_u8, encode_u64, encoded::EncodedKey, serializer::KeySerializer},
};
use reifydb_core::{
	interface::{
		catalog::{
			dictionary::Dictionary,
			flow::OperatorId,
			id::TableId,
			object::ObjectId,
			view::{View, ViewSortKey},
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::{
		catalog::serialize_object_id,
		kind::KeyKind,
		partitioned_row::{PartitionedRowKey, RowLocator},
	},
	row::row_shape_from_columns,
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_core::partition::partition_col_indices;
use reifydb_transaction::interceptor::dictionary_row::DictionaryRowInterceptor;
use reifydb_value::{
	Result,
	error::Error,
	value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber, value_type::ValueType},
};
use smallvec::smallvec;

use super::{
	coerce_columns, encode_row_at_index,
	partition::{ensure_partition_unchanged, partition_of, resolve_partition_flow},
	shape_field_columns,
};
use crate::flow::{Operator, error::FlowSinkError, operator::OperatorCell, transaction::FlowTransaction};

const CREATED_AT_CACHE_CAPACITY: usize = 16_384;

pub struct SinkTableViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: OperatorId,
	view: ResolvedView,
	underlying: TableId,

	key_prefix: Vec<u8>,
	partitioned_prefix: Vec<u8>,
	shape: RowShape,
	sort: Vec<ViewSortKey>,
	partition_indices: Vec<usize>,
	verified_partitions: UnsafeCell<HashMap<Partition, Vec<Value>>>,
	created_at: UnsafeCell<HashMap<RowNumber, DateTime>>,
}

impl SinkTableViewOperator {
	pub fn new(
		parent: OperatorCell,
		node: OperatorId,
		view: ResolvedView,
		underlying: TableId,
		partition_by: Vec<String>,
	) -> Self {
		let mut key_prefix: Vec<u8> = Vec::with_capacity(10);
		key_prefix.push(encode_u8(KeyKind::Row as u8));
		serialize_object_id(&ObjectId::table(underlying), &mut key_prefix);
		let mut partitioned_prefix: Vec<u8> = Vec::with_capacity(10);
		partitioned_prefix.push(encode_u8(KeyKind::PartitionedRow as u8));
		serialize_object_id(&ObjectId::table(underlying), &mut partitioned_prefix);
		let shape = row_shape_from_columns(RowFamily::Table, view.def().columns());
		let sort = view.def().sort().to_vec();
		let partition_indices = partition_col_indices(view.def().columns(), &partition_by);
		Self {
			parent,
			node,
			view,
			underlying,
			key_prefix,
			partitioned_prefix,
			shape,
			sort,
			partition_indices,
			verified_partitions: UnsafeCell::new(HashMap::new()),
			created_at: UnsafeCell::new(HashMap::new()),
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

	#[allow(clippy::mut_from_ref)]
	fn created_at_cache(&self) -> &mut HashMap<RowNumber, DateTime> {
		unsafe { &mut *self.created_at.get() }
	}

	#[inline]
	fn row_key(&self, row: RowNumber) -> EncodedKey {
		let mut buf = Vec::with_capacity(self.key_prefix.len() + 9);
		buf.extend_from_slice(&self.key_prefix);
		buf.extend_from_slice(&encode_u64(row.0));
		EncodedKey::new(buf)
	}

	#[inline]
	fn clustered_key(&self, cols: &Columns, row_idx: usize, row: RowNumber) -> EncodedKey {
		if self.sort.is_empty() {
			return self.row_key(row);
		}
		let mut serializer = KeySerializer::new();
		serializer.extend_raw(&self.key_prefix);
		for key in &self.sort {
			let value = cols.data_at(key.column.0 as usize).get_value(row_idx);
			serializer.extend_value_with_direction(&value, key.direction.clone().into());
		}
		serializer.extend_raw(&row.0.to_be_bytes());
		serializer.to_encoded_key()
	}

	#[inline]
	fn partitioned_key(&self, cols: &Columns, row_idx: usize, partition: Partition, row: RowNumber) -> EncodedKey {
		if self.sort.is_empty() {
			return PartitionedRowKey::encoded(
				ObjectId::table(self.underlying),
				partition,
				RowLocator::Row(row),
			);
		}
		let mut serializer = KeySerializer::new();
		serializer.extend_raw(&self.partitioned_prefix);
		serializer.extend_u128(partition.0);
		for key in &self.sort {
			let value = cols.data_at(key.column.0 as usize).get_value(row_idx);
			serializer.extend_value_with_direction(&value, key.direction.clone().into());
		}
		serializer.extend_raw(&row.0.to_be_bytes());
		serializer.to_encoded_key()
	}
}

impl Operator for SinkTableViewOperator {
	fn id(&self) -> OperatorId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def();
		let shape = &self.shape;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_table_view_insert(txn, view, shape, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_table_view_update(txn, view, shape, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_table_view_remove(txn, view, pre)?,
			}
		}

		Ok(Change::from_flow(self.node, change.version, Vec::new(), change.changed_at))
	}
}

impl SinkTableViewOperator {
	#[inline]
	fn apply_table_view_insert(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
		post: &Columns,
	) -> Result<()> {
		let coerced = coerce_columns(post, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let field_columns = shape_field_columns(source, shape);
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_rows: Vec<EncodedBytes> = Vec::with_capacity(row_count);

		let verified = self.verified_partitions();
		let cache = self.created_at_cache();
		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
				resolve_partition_flow(
					txn,
					ObjectId::table(self.underlying),
					partition,
					&values,
					verified,
				)?;
				self.partitioned_key(source, row_idx, partition, row_number)
			} else {
				self.clustered_key(source, row_idx, row_number)
			};
			remember_created_at(cache, row_number, read_created_at(&encoded));
			keys.push(key);
			encoded_rows.push(encoded);
		}

		txn.set_batch(&keys, &encoded_rows)?;

		emit_view_change(txn, view, Diff::insert(coerced));
		Ok(())
	}

	#[inline]
	fn apply_table_view_update(
		&self,
		txn: &mut FlowTransaction,
		view: &View,
		shape: &RowShape,
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
		let mut post_encoded_rows: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		let verified = self.verified_partitions();
		let cache = self.created_at_cache();
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers()[row_idx];
			let post_row_number = source_post.row_numbers()[row_idx];
			let (_, mut post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				ensure_partition_unchanged(
					ObjectId::table(self.underlying),
					pre_partition,
					post_partition,
				)?;
				resolve_partition_flow(
					txn,
					ObjectId::table(self.underlying),
					post_partition,
					&post_values,
					verified,
				)?;
				(
					self.partitioned_key(source_pre, row_idx, pre_partition, pre_row_number),
					self.partitioned_key(source_post, row_idx, post_partition, post_row_number),
				)
			} else {
				(
					self.clustered_key(source_pre, row_idx, pre_row_number),
					self.clustered_key(source_post, row_idx, post_row_number),
				)
			};

			let mut prior_created = cache.get(&post_row_number).copied().filter(|c| !c.is_epoch());
			if prior_created.is_none() && pre_row_number != post_row_number {
				prior_created = cache.get(&pre_row_number).copied().filter(|c| !c.is_epoch());
			}
			if prior_created.is_none() {
				prior_created = match txn.get(&post_key)? {
					Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
						let c = read_created_at(prior.bytes());
						if !c.is_epoch() {
							Some(c)
						} else {
							None
						}
					}
					_ => None,
				};
				if prior_created.is_none() && pre_key.as_slice() != post_key.as_slice() {
					prior_created = match txn.get(&pre_key)? {
						Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
							let c = read_created_at(prior.bytes());
							if !c.is_epoch() {
								Some(c)
							} else {
								None
							}
						}
						_ => None,
					};
				}
			}
			if let Some(c) = prior_created
				&& post_encoded.len() >= SHAPE_HEADER_SIZE
			{
				let updated = read_updated_at(&post_encoded);
				let mut builder = EncodedTableRow::from(post_encoded).thaw();
				builder.set_timestamps(c, updated);
				post_encoded = builder.freeze_bytes();
			}

			if pre_row_number != post_row_number {
				cache.remove(&pre_row_number);
			}
			remember_created_at(cache, post_row_number, read_created_at(&post_encoded));

			pre_keys.push(pre_key);
			post_keys.push(post_key);
			post_encoded_rows.push(post_encoded);
		}

		txn.remove_batch(&pre_keys)?;
		txn.set_batch(&post_keys, &post_encoded_rows)?;

		emit_view_change(txn, view, Diff::update(coerced_pre, coerced_post));
		Ok(())
	}

	#[inline]
	fn apply_table_view_remove(&self, txn: &mut FlowTransaction, view: &View, pre: &Columns) -> Result<()> {
		let coerced = coerce_columns(pre, view.columns())?;
		let dict_encoded = dictionary_encode_view_columns(txn, view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.row_count();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let cache = self.created_at_cache();
		for row_idx in 0..row_count {
			let row_number = source.row_numbers()[row_idx];
			cache.remove(&row_number);
			let key = if self.is_partitioned() {
				let (partition, _values) = partition_of(&self.partition_indices, &coerced, row_idx);
				self.partitioned_key(source, row_idx, partition, row_number)
			} else {
				self.clustered_key(source, row_idx, row_number)
			};
			keys.push(key);
		}

		txn.remove_batch(&keys)?;

		emit_view_change(txn, view, Diff::remove(coerced));
		Ok(())
	}
}

fn remember_created_at(cache: &mut HashMap<RowNumber, DateTime>, row_number: RowNumber, created_at: DateTime) {
	if created_at.is_epoch() {
		return;
	}
	if cache.len() >= CREATED_AT_CACHE_CAPACITY {
		cache.clear();
	}
	cache.insert(row_number, created_at);
}

#[inline]
fn emit_view_change(txn: &mut FlowTransaction, view: &View, diff: Diff) {
	let version = txn.version();
	let changed_at = DateTime::from_nanos(txn.clock().now().to_nanos());
	txn.track_flow_change(Change {
		origin: ChangeOrigin::Object(ObjectId::view(view.id())),
		version,
		diffs: smallvec![diff],
		changed_at,
	});
}

pub(crate) fn dictionary_encode_view_columns(
	txn: &mut FlowTransaction,
	view: &View,
	columns: &Columns,
) -> Result<Option<Columns>> {
	let mut dict_columns: Vec<(usize, Dictionary)> = Vec::new();
	{
		let catalog = txn.catalog();
		for (pos, col) in view.columns().iter().enumerate() {
			if let Some(dict_id) = col.dictionary_id {
				let dictionary = catalog.cache().find_dictionary(dict_id).ok_or_else(|| {
					Error::from(FlowSinkError::DictionaryNotFound {
						dictionary_id: format!("{:?}", dict_id),
						column: col.name.to_string(),
					})
				})?;
				dict_columns.push((pos, dictionary));
			}
		}
	}

	if dict_columns.is_empty() {
		return Ok(None);
	}

	let mut encoded = columns.clone();
	for (col_pos, dictionary) in &dict_columns {
		let row_count = encoded[*col_pos].len();

		let mut values: Vec<Value> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let mut values_buf = [encoded[*col_pos].get_value(row_idx)];
			DictionaryRowInterceptor::pre_insert(txn, dictionary, &mut values_buf)?;
			let [value] = values_buf;
			values.push(value);
		}

		let registry = txn.dictionary_allocators();
		let outcomes = registry.intern_batch(dictionary, &values)?;

		let mut new_data = ColumnBuffer::with_capacity(ValueType::DictionaryId, row_count);
		for outcome in &outcomes {
			new_data.push_value(outcome.id.to_value());
		}
		encoded.columns[*col_pos] = new_data;
	}

	Ok(Some(encoded))
}
