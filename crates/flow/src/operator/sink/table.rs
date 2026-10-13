// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		bytes::{EncodedBytes, read_created_at},
		shape::{RowFamily, RowShape},
	},
};
use reifydb_core::{
	interface::{
		catalog::{
			dictionary::Dictionary,
			flow::OperatorId,
			object::ObjectId,
			storage::StorageId,
			view::{View, ViewSortKey},
		},
		change::{Change, Diff},
	},
	internal_err,
	partition::partition_col_indices,
	row::row_shape_from_columns,
	value::{
		batch::batch,
		column::{
			dictionary::{gather_dictionary_values, scatter_dictionary_ids},
			factory::dictionary_id,
		},
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		partition::Partition,
		row_number::RowNumber,
		system_columns::{created_at, require_row_numbers},
	},
};
use tracing::instrument;

use crate::{
	error::FlowSinkError,
	operator::sink::{
		SourceRowEncoder, coerce_columns,
		partition::{ensure_partition_unchanged, partition_batch, resolve_partitions_flow},
		shape_field_columns, stored_created_at_many,
		view::{partitioned_key, sort_runs, sorted_view_key},
	},
	txn::{Batch, Emit, Get, Intern},
};

const CREATED_AT_CACHE_CAPACITY: usize = 16_384;

pub struct TableSink {
	operator: OperatorId,
	view: View,
	storage: StorageId,
	shape: RowShape,
	sort: Vec<ViewSortKey>,
	partition_indices: Vec<usize>,
	verified_partitions: HashMap<Partition, Vec<Value>>,
	created_at: HashMap<RowNumber, DateTime>,
	runtime_context: RuntimeContext,
}

impl TableSink {
	pub fn new(operator: OperatorId, view: View, runtime_context: RuntimeContext) -> Self {
		let storage = view.storage_id();
		let shape = row_shape_from_columns(RowFamily::Table, view.columns());
		let sort = view.sort().to_vec();
		let partition_indices = partition_col_indices(view.columns(), view.partition_by());
		Self {
			operator,
			view,
			storage,
			shape,
			sort,
			partition_indices,
			verified_partitions: HashMap::new(),
			created_at: HashMap::new(),
			runtime_context,
		}
	}

	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn apply<T: Get + Batch + Emit + Intern>(&mut self, txn: &mut T, change: Change) -> Result<()> {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => self.apply_insert(txn, post)?,
				Diff::Update {
					pre,
					post,
					..
				} => self.apply_update(txn, pre, post)?,
				Diff::Remove {
					pre,
					..
				} => self.apply_remove(txn, pre)?,
			}
		}
		Ok(())
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
	}

	fn resolved_partitions<T: Get + Batch>(
		&mut self,
		txn: &mut T,
		source: &RecordBatch,
	) -> Result<Option<Vec<Partition>>> {
		if !self.is_partitioned() {
			return Ok(None);
		}
		let partitions = partition_batch(&self.view, &self.partition_indices, source)?;
		resolve_partitions_flow(
			txn,
			ObjectId::from(self.storage),
			&partitions.distinct,
			&mut self.verified_partitions,
		)?;
		Ok(Some(partitions.rows))
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::view::insert", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_insert<T: Get + Batch + Emit + Intern>(&mut self, txn: &mut T, post: &RecordBatch) -> Result<()> {
		if post.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(post, self.view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_encode_view_columns(txn, &self.view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.num_rows();
		let field_columns = shape_field_columns(source, &self.shape);
		let encoder = SourceRowEncoder::new(source, &self.shape, &field_columns)?;
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let mut encoded_bytes_list: Vec<EncodedBytes> = Vec::with_capacity(row_count);
		let row_numbers = require_row_numbers(source)?;
		let mut runs = sort_runs(&self.sort, &coerced)?.into_iter();
		let encoded = encoder.encode_all()?;
		let partitions = self.resolved_partitions(txn, source)?;

		for ((row_idx, &row_number), encoded) in row_numbers.iter().enumerate().take(row_count).zip(encoded) {
			let key = if let Some(partitions) = &partitions {
				partitioned_key(self.storage, runs.next(), partitions[row_idx], row_number)
			} else {
				sorted_view_key(self.storage, runs.next(), row_number)
			};
			remember_created_at(&mut self.created_at, row_number, read_created_at(&encoded));
			keys.push(key);
			encoded_bytes_list.push(encoded);
		}

		txn.set(keys, encoded_bytes_list)?;

		txn.emit(self.view.id(), Diff::insert(coerced))
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::view::update", level = "trace", skip_all, fields(rows = post.num_rows()))]
	fn apply_update<T: Get + Batch + Emit + Intern>(
		&mut self,
		txn: &mut T,
		pre: &RecordBatch,
		post: &RecordBatch,
	) -> Result<()> {
		if pre.num_rows() == 0 && post.num_rows() == 0 {
			return Ok(());
		}
		let coerced_pre = coerce_columns(pre, self.view.columns(), &self.runtime_context)?;
		let coerced_post = coerce_columns(post, self.view.columns(), &self.runtime_context)?;
		let dict_pre = if self.is_partitioned() {
			dictionary_lookup_partition_columns(txn, &self.view, &self.partition_indices, &coerced_pre)?
		} else {
			None
		};
		let dict_post = dictionary_encode_view_columns(txn, &self.view, &coerced_post)?;
		let source_pre = dict_pre.as_ref().unwrap_or(&coerced_pre);
		let source_post = dict_post.as_ref().unwrap_or(&coerced_post);
		let row_count = source_post.num_rows();
		let (pre_row_numbers, post_row_numbers) =
			(require_row_numbers(source_pre)?, require_row_numbers(source_post)?);
		let mut pre_runs = sort_runs(&self.sort, &coerced_pre)?.into_iter();
		let mut post_runs = sort_runs(&self.sort, &coerced_post)?.into_iter();
		let partitions = if self.is_partitioned() {
			let pre = partition_batch(&self.view, &self.partition_indices, source_pre)?;
			let post = partition_batch(&self.view, &self.partition_indices, source_post)?;
			for (&pre_partition, &post_partition) in pre.rows.iter().zip(&post.rows) {
				ensure_partition_unchanged(
					ObjectId::from(self.storage),
					pre_partition,
					post_partition,
				)?;
			}
			resolve_partitions_flow(
				txn,
				ObjectId::from(self.storage),
				&post.distinct,
				&mut self.verified_partitions,
			)?;
			Some(post.rows)
		} else {
			None
		};
		let mut keys: Vec<(EncodedKey, EncodedKey)> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let (pre_row_number, post_row_number) = (pre_row_numbers[row_idx], post_row_numbers[row_idx]);
			keys.push(if let Some(partitions) = &partitions {
				let partition = partitions[row_idx];
				(
					partitioned_key(self.storage, pre_runs.next(), partition, pre_row_number),
					partitioned_key(self.storage, post_runs.next(), partition, post_row_number),
				)
			} else {
				(
					sorted_view_key(self.storage, pre_runs.next(), pre_row_number),
					sorted_view_key(self.storage, post_runs.next(), post_row_number),
				)
			});
		}

		let may_clear = self.created_at.len() + row_count >= CREATED_AT_CACHE_CAPACITY;
		let mut reads: Vec<(EncodedKey, Option<EncodedKey>)> = Vec::new();
		let mut read_slots: Vec<Option<usize>> = Vec::with_capacity(row_count);
		for (row_idx, (pre_key, post_key)) in keys.iter().enumerate() {
			if !may_clear && cached_created_at(&self.created_at, post_row_numbers[row_idx]).is_some() {
				read_slots.push(None);
				continue;
			}
			read_slots.push(Some(reads.len()));
			let pre = (pre_key.as_slice() != post_key.as_slice()).then(|| pre_key.clone());
			reads.push((post_key.clone(), pre));
		}
		let stored = stored_created_at_many(txn, &reads)?;

		let incoming = created_at(source_post)?;
		let mut created: Vec<DateTime> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let post_row_number = post_row_numbers[row_idx];
			let prior = match cached_created_at(&self.created_at, post_row_number) {
				Some(cached) => Some(cached),
				None => {
					let Some(slot) = read_slots[row_idx] else {
						return internal_err!(
							"view row {} lost its cached created_at without a stored read",
							post_row_number.0
						);
					};
					stored[slot]
				}
			};
			let Some(stamp) = prior.or_else(|| incoming.get(row_idx).copied()) else {
				return Err(Error::from(FlowSinkError::MissingSystemColumn {
					column: "created_at",
					row_idx,
				}));
			};
			remember_created_at(&mut self.created_at, post_row_number, stamp);
			created.push(stamp);
		}

		let field_columns = shape_field_columns(source_post, &self.shape);
		let encoded = SourceRowEncoder::new(source_post, &self.shape, &field_columns)?
			.with_created_at(&created)
			.encode_all()?;
		let mut writes = KeyWrites::with_capacity(row_count);
		for ((pre_key, post_key), post_encoded) in keys.into_iter().zip(encoded) {
			if pre_key.as_slice() != post_key.as_slice() {
				writes.put(pre_key, None);
			}
			writes.put(post_key, Some(post_encoded));
		}

		let (removes, set_keys, set_rows) = writes.split();
		if !removes.is_empty() {
			txn.remove(removes)?;
		}
		txn.set(set_keys, set_rows)?;

		txn.emit(self.view.id(), Diff::update(coerced_pre, coerced_post))
	}

	#[inline]
	#[instrument(name = "flow::operator::sink::view::remove", level = "trace", skip_all, fields(rows = pre.num_rows()))]
	fn apply_remove<T: Get + Batch + Emit + Intern>(&mut self, txn: &mut T, pre: &RecordBatch) -> Result<()> {
		if pre.num_rows() == 0 {
			return Ok(());
		}
		let coerced = coerce_columns(pre, self.view.columns(), &self.runtime_context)?;
		let dict_encoded = dictionary_lookup_view_columns(txn, &self.view, &coerced)?;
		let source = dict_encoded.as_ref().unwrap_or(&coerced);
		let row_count = source.num_rows();
		let mut keys: Vec<EncodedKey> = Vec::with_capacity(row_count);
		let row_numbers = require_row_numbers(source)?;
		let mut runs = sort_runs(&self.sort, &coerced)?.into_iter();
		let partitions = if self.is_partitioned() {
			Some(partition_batch(&self.view, &self.partition_indices, source)?.rows)
		} else {
			None
		};
		for (row_idx, &row_number) in row_numbers.iter().enumerate().take(row_count) {
			self.created_at.remove(&row_number);
			let key = if let Some(partitions) = &partitions {
				partitioned_key(self.storage, runs.next(), partitions[row_idx], row_number)
			} else {
				sorted_view_key(self.storage, runs.next(), row_number)
			};
			keys.push(key);
		}

		txn.remove(keys)?;

		txn.emit(self.view.id(), Diff::remove(coerced))
	}
}

fn cached_created_at(cache: &HashMap<RowNumber, DateTime>, row_number: RowNumber) -> Option<DateTime> {
	cache.get(&row_number).copied().filter(|c| !c.is_epoch())
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

fn view_dictionaries<T: Intern + ?Sized>(txn: &mut T, view: &View) -> Result<Vec<(usize, Dictionary)>> {
	let mut dict_columns: Vec<(usize, Dictionary)> = Vec::new();
	for (pos, col) in view.columns().iter().enumerate() {
		if let Some(dict_id) = col.dictionary_id {
			let dictionary = txn.dictionary(dict_id)?.ok_or_else(|| {
				Error::from(FlowSinkError::DictionaryNotFound {
					dictionary_id: format!("{:?}", dict_id),
					column: col.name.to_string(),
				})
			})?;
			dict_columns.push((pos, dictionary));
		}
	}
	Ok(dict_columns)
}

pub fn dictionary_encode_view_columns<T: Intern + ?Sized>(
	txn: &mut T,
	view: &View,
	columns: &RecordBatch,
) -> Result<Option<RecordBatch>> {
	let dict_columns = view_dictionaries(txn, view)?;
	if dict_columns.is_empty() {
		return Ok(None);
	}

	let mut encoded = pairs(columns);
	for (col_pos, dictionary) in &dict_columns {
		let (field, array) = &encoded[*col_pos];
		let views = [ColumnView::try_from((array, field.as_ref()))?];
		let mut values = Vec::new();
		gather_dictionary_values(&views, &mut values);
		let ids = txn.intern(dictionary, &values)?;
		let ids = scatter_dictionary_ids(&views, dictionary.id_type.none(), &mut ids.into_iter()).remove(0);
		encoded[*col_pos] = dictionary_id(field.name(), ids);
	}

	Ok(Some(batch(encoded)?))
}

pub fn dictionary_lookup_view_columns<T: Intern + ?Sized>(
	txn: &mut T,
	view: &View,
	columns: &RecordBatch,
) -> Result<Option<RecordBatch>> {
	let dict_columns = view_dictionaries(txn, view)?;
	lookup_dictionary_columns(txn, view, columns, dict_columns)
}

pub fn dictionary_lookup_partition_columns<T: Intern + ?Sized>(
	txn: &mut T,
	view: &View,
	partition_indices: &[usize],
	columns: &RecordBatch,
) -> Result<Option<RecordBatch>> {
	let mut dict_columns = view_dictionaries(txn, view)?;
	dict_columns.retain(|(pos, _)| partition_indices.contains(pos));
	lookup_dictionary_columns(txn, view, columns, dict_columns)
}

fn lookup_dictionary_columns<T: Intern + ?Sized>(
	txn: &mut T,
	view: &View,
	columns: &RecordBatch,
	dict_columns: Vec<(usize, Dictionary)>,
) -> Result<Option<RecordBatch>> {
	if dict_columns.is_empty() {
		return Ok(None);
	}

	let mut encoded = pairs(columns);
	for (col_pos, dictionary) in &dict_columns {
		let (field, array) = &encoded[*col_pos];
		let views = [ColumnView::try_from((array, field.as_ref()))?];
		let mut values = Vec::new();
		gather_dictionary_values(&views, &mut values);
		let ids = if values.is_empty() {
			Vec::new()
		} else {
			txn.find(dictionary, &values)?
		};
		if ids.len() != values.len() {
			return internal_err!(
				"a dictionary batch find answered {} ids for {} values",
				ids.len(),
				values.len()
			);
		}
		let ids = ids
			.into_iter()
			.map(|id| {
				id.ok_or_else(|| {
					Error::from(FlowSinkError::DictionaryEntryNotFound {
						dictionary_id: format!("{:?}", dictionary.id),
						column: view.columns()[*col_pos].name.to_string(),
					})
				})
			})
			.collect::<Result<Vec<_>>>()?;
		let ids = scatter_dictionary_ids(&views, dictionary.id_type.none(), &mut ids.into_iter()).remove(0);
		encoded[*col_pos] = dictionary_id(field.name(), ids);
	}

	Ok(Some(batch(encoded)?))
}

fn pairs(columns: &RecordBatch) -> Vec<(FieldRef, ArrayRef)> {
	columns.schema_ref().fields().iter().cloned().zip(columns.columns().iter().cloned()).collect()
}

struct KeyWrites {
	writes: Vec<(EncodedKey, Option<EncodedBytes>)>,
	slots: HashMap<EncodedKey, usize>,
}

impl KeyWrites {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			writes: Vec::with_capacity(capacity * 2),
			slots: HashMap::with_capacity(capacity * 2),
		}
	}

	fn put(&mut self, key: EncodedKey, row: Option<EncodedBytes>) {
		match self.slots.get(&key) {
			Some(&slot) => self.writes[slot].1 = row,
			None => {
				self.slots.insert(key.clone(), self.writes.len());
				self.writes.push((key, row));
			}
		}
	}

	fn split(self) -> (Vec<EncodedKey>, Vec<EncodedKey>, Vec<EncodedBytes>) {
		let mut removes = Vec::new();
		let mut set_keys = Vec::with_capacity(self.writes.len());
		let mut set_rows = Vec::with_capacity(self.writes.len());
		for (key, row) in self.writes {
			match row {
				Some(row) => {
					set_keys.push(key);
					set_rows.push(row);
				}
				None => removes.push(key),
			}
		}
		(removes, set_keys, set_rows)
	}
}
