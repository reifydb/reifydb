// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::to_stdvec;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::RowShape,
	},
	interface::{
		catalog::{
			dictionary::Dictionary,
			flow::FlowNodeId,
			id::TableId,
			shape::ShapeId,
			view::{View, ViewSortKey},
		},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	internal_error,
	key::{
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionarySequenceKey},
		kind::KeyKind,
	},
	util::encoding::keycode::{
		catalog::serialize_shape_id, encode_u8, encode_u64_varint, serializer::KeySerializer,
	},
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_runtime::hash::xxh3_128;
use reifydb_value::{
	Result,
	util::cowvec::CowVec,
	value::{
		Value, datetime::DateTime, dictionary::DictionaryEntryId, row_number::RowNumber, value_type::ValueType,
	},
};
use smallvec::smallvec;

use super::{coerce_columns, encode_row_at_index, shape_field_columns};
use crate::{Operator, operator::OperatorCell, transaction::FlowTransaction};

pub struct SinkTableViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	view: ResolvedView,

	key_prefix: Vec<u8>,
	shape: RowShape,
	sort: Vec<ViewSortKey>,
}

impl SinkTableViewOperator {
	pub fn new(parent: OperatorCell, node: FlowNodeId, view: ResolvedView, underlying: TableId) -> Self {
		let mut key_prefix: Vec<u8> = Vec::with_capacity(10);
		key_prefix.push(encode_u8(KeyKind::Row as u8));
		serialize_shape_id(&ShapeId::table(underlying), &mut key_prefix);
		let shape: RowShape = view.def().columns().into();
		let sort = view.def().sort().to_vec();
		Self {
			parent,
			node,
			view,
			key_prefix,
			shape,
			sort,
		}
	}

	#[inline]
	fn row_key(&self, row: RowNumber) -> EncodedKey {
		let mut buf = Vec::with_capacity(self.key_prefix.len() + 9);
		buf.extend_from_slice(&self.key_prefix);
		encode_u64_varint(row.0, &mut buf);
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
			serializer.extend_value_with_direction(&value, key.direction.clone());
		}
		serializer.extend_raw(&row.0.to_be_bytes());
		serializer.to_encoded_key()
	}
}

impl Operator for SinkTableViewOperator {
	fn id(&self) -> FlowNodeId {
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
		let mut encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);

		for row_idx in 0..row_count {
			let row_number = source.row_numbers[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			keys.push(self.clustered_key(source, row_idx, row_number));
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
		let mut post_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers[row_idx];
			let post_row_number = source_post.row_numbers[row_idx];
			let (_, mut post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			let pre_key = self.clustered_key(source_pre, row_idx, pre_row_number);
			let post_key = self.clustered_key(source_post, row_idx, post_row_number);

			let prior_created = match txn.get(&post_key)? {
				Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
					let c = prior.created_at_nanos();
					if c != 0 {
						Some(c)
					} else {
						None
					}
				}
				_ => None,
			};
			if prior_created.is_none() && pre_key.as_slice() != post_key.as_slice() {
				match txn.get(&pre_key)? {
					Some(prior) if prior.len() >= SHAPE_HEADER_SIZE => {
						let c = prior.created_at_nanos();
						if c != 0 && post_encoded.len() >= SHAPE_HEADER_SIZE {
							let updated = post_encoded.updated_at_nanos();
							post_encoded.set_timestamps(c, updated);
						}
					}
					_ => {}
				}
			} else if let Some(c) = prior_created
				&& post_encoded.len() >= SHAPE_HEADER_SIZE
			{
				let updated = post_encoded.updated_at_nanos();
				post_encoded.set_timestamps(c, updated);
			}

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
		for row_idx in 0..row_count {
			let row_number = source.row_numbers[row_idx];
			keys.push(self.clustered_key(source, row_idx, row_number));
		}

		txn.remove_batch(&keys)?;

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
					internal_error!(
						"Dictionary {:?} not found for view column {}",
						dict_id,
						col.name
					)
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
		let mut new_data = ColumnBuffer::with_capacity(ValueType::DictionaryId, row_count);
		for row_idx in 0..row_count {
			let value = encoded[*col_pos].get_value(row_idx);
			let entry_id = dictionary_intern(txn, dictionary, &value)?;
			new_data.push_value(entry_id.to_value());
		}
		encoded.columns.make_mut()[*col_pos] = new_data;
	}

	Ok(Some(encoded))
}

fn dictionary_intern(txn: &mut FlowTransaction, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId> {
	let value_bytes = to_stdvec(value).map_err(|e| internal_error!("Failed to serialize value: {}", e))?;
	let hash = xxh3_128(&value_bytes).0.to_be_bytes();

	let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
	if let Some(existing) = txn.get(&entry_key)? {
		let id = u128::from_be_bytes(existing[..16].try_into().unwrap());
		return DictionaryEntryId::from_u128(id, dictionary.id_type.clone());
	}

	let seq_key = DictionarySequenceKey::encoded(dictionary.id);
	let next_id = match txn.get(&seq_key)? {
		Some(v) => u128::from_be_bytes(v[..16].try_into().unwrap()) + 1,
		None => 1,
	};

	let entry_id = DictionaryEntryId::from_u128(next_id, dictionary.id_type.clone())?;

	let mut entry_value = Vec::with_capacity(16 + value_bytes.len());
	entry_value.extend_from_slice(&next_id.to_be_bytes());
	entry_value.extend_from_slice(&value_bytes);
	txn.set(&entry_key, EncodedRow(CowVec::new(entry_value)))?;

	let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, next_id as u64);
	txn.set(&index_key, EncodedRow(CowVec::new(value_bytes)))?;

	txn.set(&seq_key, EncodedRow(CowVec::new(next_id.to_be_bytes().to_vec())))?;

	Ok(entry_id)
}
