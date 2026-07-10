// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use postcard::to_stdvec;
use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::{
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::RowShape,
	},
	key::{encode_u8, encode_u64_varint, encoded::EncodedKey, serializer::KeySerializer},
};
use reifydb_core::{
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
	key::{
		catalog::serialize_shape_id,
		kind::KeyKind,
		partitioned_row::{PartitionedRowKey, RowLocator},
	},
	row::row_shape_from_columns,
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::partition::partition_col_indices;
use reifydb_transaction::interceptor::dictionary_row::DictionaryRowInterceptor;
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value, datetime::DateTime, dictionary::DictionaryEntryId, partition::Partition, row_number::RowNumber,
		value_type::ValueType,
	},
};
use smallvec::smallvec;

use super::{
	coerce_columns, encode_row_at_index,
	partition::{partition_of, resolve_partition_flow},
	shape_field_columns,
};
use crate::{
	Operator,
	error::{FlowSinkError, FlowStateError},
	operator::OperatorCell,
	transaction::FlowTransaction,
};

pub struct SinkTableViewOperator {
	#[allow(dead_code)]
	parent: OperatorCell,
	node: FlowNodeId,
	view: ResolvedView,
	underlying: TableId,

	key_prefix: Vec<u8>,
	partitioned_prefix: Vec<u8>,
	shape: RowShape,
	sort: Vec<ViewSortKey>,
	partition_indices: Vec<usize>,
}

impl SinkTableViewOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		view: ResolvedView,
		underlying: TableId,
		partition_by: Vec<String>,
	) -> Self {
		let mut key_prefix: Vec<u8> = Vec::with_capacity(10);
		key_prefix.push(encode_u8(KeyKind::Row as u8));
		serialize_shape_id(&ShapeId::table(underlying), &mut key_prefix);
		let mut partitioned_prefix: Vec<u8> = Vec::with_capacity(10);
		partitioned_prefix.push(encode_u8(KeyKind::PartitionedRow as u8));
		serialize_shape_id(&ShapeId::table(underlying), &mut partitioned_prefix);
		let shape = row_shape_from_columns(view.def().columns());
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
		}
	}

	#[inline]
	fn is_partitioned(&self) -> bool {
		!self.partition_indices.is_empty()
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
			serializer.extend_value_with_direction(&value, key.direction.clone().into());
		}
		serializer.extend_raw(&row.0.to_be_bytes());
		serializer.to_encoded_key()
	}

	#[inline]
	fn partitioned_key(&self, cols: &Columns, row_idx: usize, partition: Partition, row: RowNumber) -> EncodedKey {
		if self.sort.is_empty() {
			return PartitionedRowKey::encoded(
				ShapeId::table(self.underlying),
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

		let mut verified: HashSet<Partition> = HashSet::new();
		for row_idx in 0..row_count {
			let row_number = source.row_numbers[row_idx];
			let (_, encoded) = encode_row_at_index(source, row_idx, shape, row_number, &field_columns)?;
			let key = if self.is_partitioned() {
				let (partition, values) = partition_of(&self.partition_indices, &coerced, row_idx);
				resolve_partition_flow(
					txn,
					ShapeId::table(self.underlying),
					partition,
					&values,
					&mut verified,
				)?;
				self.partitioned_key(source, row_idx, partition, row_number)
			} else {
				self.clustered_key(source, row_idx, row_number)
			};
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
		let mut post_encoded_rows: Vec<EncodedRow> = Vec::with_capacity(row_count);
		let mut verified: HashSet<Partition> = HashSet::new();
		for row_idx in 0..row_count {
			let pre_row_number = source_pre.row_numbers[row_idx];
			let post_row_number = source_post.row_numbers[row_idx];
			let (_, mut post_encoded) =
				encode_row_at_index(source_post, row_idx, shape, post_row_number, &field_columns)?;

			let (pre_key, post_key) = if self.is_partitioned() {
				let (pre_partition, _pre_values) =
					partition_of(&self.partition_indices, &coerced_pre, row_idx);
				let (post_partition, post_values) =
					partition_of(&self.partition_indices, &coerced_post, row_idx);
				resolve_partition_flow(
					txn,
					ShapeId::table(self.underlying),
					post_partition,
					&post_values,
					&mut verified,
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
	let mut values_buf = [value.clone()];
	DictionaryRowInterceptor::pre_insert(txn, dictionary, &mut values_buf)?;
	let [value] = values_buf;

	let value_bytes = to_stdvec(&value).map_err(|e| {
		Error::from(FlowStateError::Encode {
			state: "value",
			cause: e.to_string(),
		})
	})?;

	let registry = txn.dictionary_allocators();
	let outcome = registry.intern(dictionary, &value_bytes, txn)?;

	if let Some(writes) = outcome.writes {
		txn.set(&writes.entry_key, writes.entry_value)?;
		txn.set(&writes.index_key, writes.index_value)?;
	}

	Ok(outcome.id)
}

#[cfg(test)]
mod tests {
	use postcard::from_bytes;
	use reifydb_core::{
		actors::pending::PendingWrite, common::CommitVersion, key::dictionary::DictionaryEntryIndexKey,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	// A deferred flow batch reads source data at its own commit version. Dictionary interning
	// state (the sequence counter and entry/index rows), however, is persisted by the coordinator
	// at a *higher* flow commit version. If a later batch's interning read resolves through that
	// pinned source-version snapshot, it misses an earlier batch's committed sequence increment,
	// computes a colliding id, and overwrites the existing index entry - so several distinct
	// strings decode to one (the production symptom: 3 view rows all reading "wsol").
	//
	// This test reproduces the split-batch ordering deterministically: phase 1 interns and
	// COMMITS two values, phase 2 interns a third through a transaction pinned to a version that
	// predates phase 1's commit. Interning must still allocate a fresh id and never clobber an
	// existing entry. The fix routes dictionary reads to the latest committed version
	// (ReadFrom::DictionaryQuery), so phase 2 observes the committed sequence.
	#[test]
	fn dictionary_intern_does_not_collide_across_a_stale_version_snapshot() {
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE test");
		t.admin("CREATE DICTIONARY test::syms FOR utf8 AS uint2");

		let engine = t.inner();
		let catalog = engine.catalog();
		let namespace = catalog.cache().find_namespace_by_name("test").expect("namespace test");
		let dictionary =
			catalog.cache().find_dictionary_by_name(namespace.id(), "syms").expect("dictionary syms");

		// Persist a deferred transaction's pending dictionary writes the way the coordinator does.
		let commit_pending = |txn: &mut FlowTransaction| {
			let pending = txn.take_pending();
			let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
			cmd.disable_conflict_tracking().unwrap();
			for (key, pw) in pending.iter_sorted() {
				match pw {
					PendingWrite::Set(v) => cmd.set(key, v.clone()).unwrap(),
					PendingWrite::Remove => cmd.remove(key).unwrap(),
					PendingWrite::Drop => cmd.drop_key(key).unwrap(),
				};
			}
			cmd.commit_unchecked().unwrap()
		};

		// Phase 1 (the INSERT batch): intern sol + usdc, then commit the pending writes.
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut insert_txn = FlowTransaction::deferred(
			&parent,
			version,
			catalog.clone(),
			engine.create_interceptors(),
			engine.clock().clone(),
		);
		let sol_id = dictionary_intern(&mut insert_txn, &dictionary, &Value::Utf8("sol".to_string()))
			.unwrap()
			.to_u128();
		let usdc_id = dictionary_intern(&mut insert_txn, &dictionary, &Value::Utf8("usdc".to_string()))
			.unwrap()
			.to_u128();
		assert_ne!(sol_id, usdc_id, "distinct strings must intern to distinct ids");
		let phase1_commit = commit_pending(&mut insert_txn);

		// Phase 2 (the UPDATE batch): a fresh deferred transaction whose source-version snapshot
		// predates phase 1's commit. (A deferred read at version V sees commits with version <= V+1,
		// so pinning two below the phase-1 commit excludes phase 1's persisted dictionary writes -
		// exactly the production split-batch situation where the UPDATE batch's source version is
		// below the flow commit that persisted the INSERT batch.) With the bug, the sequence read is
		// stale and "wsol" reuses an id already in use, overwriting that entry.
		let stale_version = CommitVersion(phase1_commit.0 - 2);
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let mut update_txn = FlowTransaction::deferred(
			&parent,
			stale_version,
			catalog.clone(),
			engine.create_interceptors(),
			engine.clock().clone(),
		);
		let wsol_id = dictionary_intern(&mut update_txn, &dictionary, &Value::Utf8("wsol".to_string()))
			.unwrap()
			.to_u128();

		assert_ne!(wsol_id, sol_id, "wsol must not reuse sol's id (would overwrite sol's entry)");
		assert_ne!(wsol_id, usdc_id, "wsol must not reuse usdc's id (would overwrite usdc's entry)");
		commit_pending(&mut update_txn);

		// Every interned string must still decode to itself - no entry was clobbered.
		let decode = |id: u128| -> String {
			let key = DictionaryEntryIndexKey::encoded(dictionary.id, id);
			let query = engine.multi().begin_query().unwrap();
			let bytes = query.get(&key).unwrap().expect("index entry present").row().to_vec();
			match from_bytes::<Value>(&bytes).unwrap() {
				Value::Utf8(s) => s,
				other => panic!("expected Utf8, got {:?}", other),
			}
		};
		assert_eq!(decode(sol_id), "sol", "sol's dictionary entry was overwritten");
		assert_eq!(decode(usdc_id), "usdc", "usdc's dictionary entry was overwritten");
		assert_eq!(decode(wsol_id), "wsol");
	}
}
