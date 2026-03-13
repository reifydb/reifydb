// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::store::ringbuffer::update::{decode_ringbuffer_metadata, encode_ringbuffer_metadata};
use reifydb_core::{
	encoded::schema::Schema,
	interface::{
		catalog::{flow::FlowNodeId, id::RingBufferId, primitive::PrimitiveId, ringbuffer::RingBufferMetadata},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	key::{ringbuffer::RingBufferMetadataKey, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::view::ViewInterceptor;
use reifydb_type::{Result, value::row_number::RowNumber};

use super::{coerce_columns, encode_row_at_index};
use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct SinkRingBufferViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
	ringbuffer_id: RingBufferId,
	capacity: u64,
	propagate_evictions: bool,
}

impl SinkRingBufferViewOperator {
	pub fn new(
		parent: Arc<Operators>,
		node: FlowNodeId,
		view: ResolvedView,
		ringbuffer_id: RingBufferId,
		capacity: u64,
		propagate_evictions: bool,
	) -> Self {
		Self {
			parent,
			node,
			view,
			ringbuffer_id,
			capacity,
			propagate_evictions,
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
}

impl Operator for SinkRingBufferViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view_def = self.view.def().clone();
		let schema: Schema = view_def.columns().into();
		let primitive_id = PrimitiveId::ringbuffer(self.ringbuffer_id);
		let mut metadata = self.read_metadata(txn)?;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let coerced = coerce_columns(post, view_def.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						// Evict oldest if full
						if metadata.is_full() {
							let oldest_row = RowNumber(metadata.head);
							let old_key = RowKey::encoded(primitive_id, oldest_row);
							txn.remove(&old_key)?;
							metadata.head += 1;
							metadata.count -= 1;

							if self.propagate_evictions {
								// We could read the old row and emit a Remove diff,
								// but for now we skip (requires reading the old value
								// from storage)
							}
						}

						let row_number = RowNumber(metadata.tail);
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, row_number);

						ViewInterceptor::pre_insert(txn, &view_def, row_number, &encoded)?;
						let key = RowKey::encoded(primitive_id, row_number);
						txn.set(&key, encoded.clone())?;
						ViewInterceptor::post_insert(txn, &view_def, row_number, &encoded)?;

						if let Some(log) = txn.testing_mut() {
							let new = Columns::single_row(coerced.iter().map(|col| {
								(col.name().text(), col.data().get_value(row_idx))
							}));
							let mutation_key = format!(
								"views::{}::{}",
								self.view.namespace().name(),
								self.view.name()
							);
							log.record_insert(mutation_key, new);
						}

						if metadata.is_empty() {
							metadata.head = row_number.0;
						}
						metadata.count += 1;
						metadata.tail = row_number.0 + 1;
					}
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id())),
						version,
						diffs: vec![Diff::Insert {
							post: coerced,
						}],
					});
				}
				Diff::Update {
					pre,
					post,
				} => {
					// Ringbuffer views support update (same as table view)
					let coerced_pre = coerce_columns(pre, view_def.columns())?;
					let coerced_post = coerce_columns(post, view_def.columns())?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let pre_row_number = coerced_pre.row_numbers[row_idx];
						let post_row_number = coerced_post.row_numbers[row_idx];
						let (_, pre_encoded) = encode_row_at_index(
							&coerced_pre,
							row_idx,
							&schema,
							pre_row_number,
						);
						let (_, post_encoded) = encode_row_at_index(
							&coerced_post,
							row_idx,
							&schema,
							post_row_number,
						);

						ViewInterceptor::pre_update(
							txn,
							&view_def,
							post_row_number,
							&post_encoded,
						)?;
						let old_key = RowKey::encoded(primitive_id, pre_row_number);
						let new_key = RowKey::encoded(primitive_id, post_row_number);
						txn.remove(&old_key)?;
						txn.set(&new_key, post_encoded.clone())?;
						ViewInterceptor::post_update(
							txn,
							&view_def,
							post_row_number,
							&post_encoded,
							&pre_encoded,
						)?;
					}
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id())),
						version,
						diffs: vec![Diff::Update {
							pre: coerced_pre,
							post: coerced_post,
						}],
					});
				}
				Diff::Remove {
					pre,
				} => {
					let coerced = coerce_columns(pre, view_def.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let row_number = coerced.row_numbers[row_idx];
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, row_number);
						ViewInterceptor::pre_delete(txn, &view_def, row_number)?;
						let key = RowKey::encoded(primitive_id, row_number);
						txn.remove(&key)?;
						ViewInterceptor::post_delete(txn, &view_def, row_number, &encoded)?;
					}
					let version = txn.version();
					txn.push_view_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view_def.id())),
						version,
						diffs: vec![Diff::Remove {
							pre: coerced,
						}],
					});
				}
			}
		}

		self.write_metadata(txn, &metadata)?;

		Ok(Change::from_flow(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!()
	}
}
