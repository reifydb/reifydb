// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use postcard::{from_bytes, to_stdvec};
use reifydb_catalog::store::ringbuffer::update::{decode_ringbuffer_metadata, encode_ringbuffer_metadata};
use reifydb_core::{
	encoded::schema::{Schema, SchemaField},
	interface::{
		catalog::{flow::FlowNodeId, id::RingBufferId, primitive::PrimitiveId, ringbuffer::RingBufferMetadata},
		change::{Change, ChangeOrigin, Diff},
		resolved::ResolvedView,
	},
	internal,
	key::{ringbuffer::RingBufferMetadataKey, row::RowKey},
	value::column::columns::Columns,
};
use reifydb_transaction::interceptor::view_row::ViewRowInterceptor;
use reifydb_type::{
	Result,
	error::Error,
	value::{blob::Blob, row_number::RowNumber, r#type::Type},
};
use serde::{Deserialize, Serialize};

use super::{coerce_columns, encode_row_at_index};
use crate::{
	Operator,
	operator::{
		Operators,
		stateful::{raw::RawStatefulOperator, single::SingleStateful},
	},
	transaction::FlowTransaction,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RingBufferState {
	forward: BTreeMap<RowNumber, RowNumber>, // source_rn → ringbuffer_key
	reverse: BTreeMap<RowNumber, RowNumber>, // ringbuffer_key → source_rn
}

pub struct SinkRingBufferViewOperator {
	#[allow(dead_code)]
	parent: Arc<Operators>,
	node: FlowNodeId,
	view: ResolvedView,
	ringbuffer_id: RingBufferId,
	capacity: u64,
	propagate_evictions: bool,
	state_schema: Schema,
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
			state_schema: Schema::new(vec![SchemaField::unconstrained("state", Type::Blob)]),
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

	fn load(&self, txn: &mut FlowTransaction) -> Result<RingBufferState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(RingBufferState::default());
		}

		let blob = self.state_schema.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(RingBufferState::default());
		}

		from_bytes(blob.as_ref()).map_err(|e| Error(internal!("Failed to deserialize RingBufferState: {}", e)))
	}

	fn save(&self, txn: &mut FlowTransaction, state: &RingBufferState) -> Result<()> {
		let serialized =
			to_stdvec(state).map_err(|e| Error(internal!("Failed to serialize RingBufferState: {}", e)))?;
		let blob = Blob::from(serialized);

		self.update_state(txn, |schema, row| {
			schema.set_blob(row, 0, &blob);
			Ok(())
		})?;
		Ok(())
	}
}

impl RawStatefulOperator for SinkRingBufferViewOperator {}

impl SingleStateful for SinkRingBufferViewOperator {
	fn layout(&self) -> Schema {
		self.state_schema.clone()
	}
}

impl Operator for SinkRingBufferViewOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let view = self.view.def().clone();
		let schema: Schema = view.columns().into();
		let primitive_id = PrimitiveId::ringbuffer(self.ringbuffer_id);
		let mut metadata = self.read_metadata(txn)?;
		let mut state = self.load(txn)?;

		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
				} => {
					let coerced = coerce_columns(post, view.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						// Evict oldest if full
						if metadata.is_full() {
							let oldest_rn = RowNumber(metadata.head);
							let old_key = RowKey::encoded(primitive_id, oldest_rn);
							txn.remove(&old_key)?;
							metadata.head += 1;
							metadata.count -= 1;

							// Clean up alias for evicted row
							if let Some(source_rn) = state.reverse.remove(&oldest_rn) {
								state.forward.remove(&source_rn);
							}

							if self.propagate_evictions {
								// We could read the old row and emit a Remove diff,
								// but for now we skip (requires reading the old value
								// from storage)
							}
						}

						let source_rn = coerced.row_numbers[row_idx];
						let assigned_rn = RowNumber(metadata.tail);
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, assigned_rn);

						// Track alias when source row number differs from assigned key
						if source_rn != assigned_rn {
							state.forward.insert(source_rn, assigned_rn);
							state.reverse.insert(assigned_rn, source_rn);
						}

						let encoded = ViewRowInterceptor::pre_insert(
							txn,
							&view,
							assigned_rn,
							encoded,
						)?;
						let key = RowKey::encoded(primitive_id, assigned_rn);
						txn.set(&key, encoded.clone())?;
						ViewRowInterceptor::post_insert(txn, &view, assigned_rn, &encoded)?;

						if metadata.is_empty() {
							metadata.head = assigned_rn.0;
						}
						metadata.count += 1;
						metadata.tail = assigned_rn.0 + 1;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view.id())),
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
					let coerced_pre = coerce_columns(pre, view.columns())?;
					let coerced_post = coerce_columns(post, view.columns())?;
					let row_count = coerced_post.row_count();
					for row_idx in 0..row_count {
						let pre_source_rn = coerced_pre.row_numbers[row_idx];
						let post_source_rn = coerced_post.row_numbers[row_idx];
						// Resolve state to storage keys
						let pre_storage_rn = state
							.forward
							.get(&pre_source_rn)
							.copied()
							.unwrap_or(pre_source_rn);
						let post_storage_rn = state
							.forward
							.get(&post_source_rn)
							.copied()
							.unwrap_or(post_source_rn);
						let (_, pre_encoded) = encode_row_at_index(
							&coerced_pre,
							row_idx,
							&schema,
							pre_storage_rn,
						);
						let (_, post_encoded) = encode_row_at_index(
							&coerced_post,
							row_idx,
							&schema,
							post_storage_rn,
						);

						let post_encoded = ViewRowInterceptor::pre_update(
							txn,
							&view,
							post_storage_rn,
							post_encoded,
						)?;
						let old_key = RowKey::encoded(primitive_id, pre_storage_rn);
						let new_key = RowKey::encoded(primitive_id, post_storage_rn);
						txn.remove(&old_key)?;
						txn.set(&new_key, post_encoded.clone())?;
						ViewRowInterceptor::post_update(
							txn,
							&view,
							post_storage_rn,
							&post_encoded,
							&pre_encoded,
						)?;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view.id())),
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
					let coerced = coerce_columns(pre, view.columns())?;
					let row_count = coerced.row_count();
					for row_idx in 0..row_count {
						let source_rn = coerced.row_numbers[row_idx];
						// Resolve alias to storage key
						let storage_rn = state.forward.remove(&source_rn).unwrap_or(source_rn);
						state.reverse.remove(&storage_rn);
						let (_, encoded) =
							encode_row_at_index(&coerced, row_idx, &schema, storage_rn);
						ViewRowInterceptor::pre_delete(txn, &view, storage_rn)?;
						let key = RowKey::encoded(primitive_id, storage_rn);
						txn.remove(&key)?;
						ViewRowInterceptor::post_delete(txn, &view, storage_rn, &encoded)?;
					}
					let version = txn.version();
					txn.track_flow_change(Change {
						origin: ChangeOrigin::Primitive(PrimitiveId::view(view.id())),
						version,
						diffs: vec![Diff::Remove {
							pre: coerced,
						}],
					});
				}
			}
		}

		self.write_metadata(txn, &metadata)?;
		self.save(txn, &state)?;

		Ok(Change::from_flow(self.node, change.version, Vec::new()))
	}

	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
		unreachable!()
	}
}
