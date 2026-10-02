// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	ringbuffer::EncodedRingBufferRow,
	shape::{RowFamily, RowShape},
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::{
			object::ObjectId,
			ringbuffer::{RingBuffer, RingBufferMetadata},
		},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey},
	},
	partition::{PartitionError, partition_col_indices, partition_of, partition_values},
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_transaction::{
	interceptor::{WithInterceptors, ringbuffer_row::RingBufferRowInterceptor},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::Result;

fn ringbuffer_key(ringbuffer: &RingBuffer, partition: Option<Partition>, row_number: RowNumber) -> TaggedKey {
	match partition {
		None => RowKey::new(ringbuffer.id, row_number).into(),
		Some(partition) => PartitionedRowKey::new(ringbuffer.id, partition, row_number).into(),
	}
}

fn ringbuffer_change(ringbuffer: &RingBuffer, diff: Diff) -> Change {
	Change {
		origin: ChangeOrigin::Object(ObjectId::ringbuffer(ringbuffer.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![diff],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_insert_change(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	ids: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<Change> {
	Ok(ringbuffer_change(ringbuffer, Diff::insert(from_encoded_bytes(shape, ids, rows)?)))
}

fn build_ringbuffer_update_change(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	ids: &[RowNumber],
	pres: &[EncodedBytes],
	posts: &[EncodedBytes],
) -> Result<Change> {
	Ok(ringbuffer_change(
		ringbuffer,
		Diff::update(from_encoded_bytes(shape, ids, pres)?, from_encoded_bytes(shape, ids, posts)?),
	))
}

fn build_ringbuffer_remove_change(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	ids: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<Change> {
	Ok(ringbuffer_change(ringbuffer, Diff::remove(from_encoded_bytes(shape, ids, rows)?)))
}

pub fn apply_ringbuffer_partition_metadata_after_delete(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	partition_key: &[Value],
	mut partition: RingBufferMetadata,
	deleted: u64,
	min_remaining_row: Option<u64>,
) -> Result<()> {
	if deleted == 0 {
		return Ok(());
	}
	let remaining_count = partition.count.saturating_sub(deleted);
	if remaining_count == 0 {
		catalog.remove_partition_metadata(txn, ringbuffer, partition_key)
	} else {
		partition.count = remaining_count;
		partition.head = min_remaining_row.unwrap();
		catalog.save_partition_metadata(txn, ringbuffer, partition_key, &partition)
	}
}

pub trait RingBufferOperations {
	fn insert_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>>;

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>>;

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
	) -> Result<Vec<EncodedBytes>>;
}

impl RingBufferOperations for CommandTransaction {
	fn insert_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		let mut stored = Vec::with_capacity(rows.len());
		let mut inserted_ids = Vec::new();
		let mut inserted = Vec::new();
		let mut replaced_ids = Vec::new();
		let mut replaced_pres = Vec::new();
		let mut replaced_posts = Vec::new();
		if ids.is_empty() {
			return Ok(stored);
		}

		let keys: Vec<TaggedKey> = ids
			.iter()
			.enumerate()
			.map(|(idx, &row_number)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number))
			.collect();
		let mut pres = Vec::with_capacity(keys.len());
		for key in &keys {
			pres.push(self.get(key)?.map(|v| v.bytes));
		}

		for (&row_number, pre) in ids.iter().zip(&pres) {
			if let Some(pre) = pre {
				replaced_ids.push(row_number);
				replaced_pres.push(pre.clone());
			}
		}
		if !replaced_ids.is_empty() {
			if !self.ringbuffer_row_pre_delete_interceptors().is_empty() {
				RingBufferRowInterceptor::pre_delete(self, ringbuffer, &replaced_ids)?;
			}
			if !self.ringbuffer_row_post_delete_interceptors().is_empty() {
				RingBufferRowInterceptor::post_delete(self, ringbuffer, &replaced_ids, &replaced_pres)?;
			}
		}

		let written: Vec<EncodedBytes> = if self.ringbuffer_row_pre_insert_interceptors().is_empty() {
			rows.to_vec()
		} else {
			let mut builders: Vec<_> =
				rows.iter().map(|bytes| EncodedRingBufferRow::from(bytes.clone()).thaw()).collect();
			RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut builders)?;
			builders.into_iter().map(|builder| builder.freeze_bytes()).collect()
		};

		for (key, bytes) in keys.iter().zip(&written) {
			self.set(key, bytes.clone())?;
		}

		if !self.ringbuffer_row_post_insert_interceptors().is_empty() {
			RingBufferRowInterceptor::post_insert(self, ringbuffer, ids, &written)?;
		}

		for ((&row_number, pre), bytes) in ids.iter().zip(&pres).zip(written) {
			match pre {
				Some(_) => replaced_posts.push(bytes.clone()),
				None => {
					inserted_ids.push(row_number);
					inserted.push(bytes.clone());
				}
			}
			stored.push(bytes);
		}

		if !inserted_ids.is_empty() {
			self.track_flow_change(build_ringbuffer_insert_change(
				ringbuffer,
				shape,
				&inserted_ids,
				&inserted,
			)?);
		}
		if !replaced_ids.is_empty() {
			let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				&shape,
				&replaced_ids,
				&replaced_pres,
				&replaced_posts,
			)?);
		}

		Ok(stored)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
		let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
		let mut updated_ids = Vec::new();
		let mut pres = Vec::new();
		let mut matched = Vec::new();

		let keys: Vec<TaggedKey> = ids
			.iter()
			.enumerate()
			.map(|(idx, &id)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id))
			.collect();
		for (idx, key) in keys.iter().enumerate() {
			if let Some(pre) = self.get(key)? {
				matched.push(idx);
				updated_ids.push(ids[idx]);
				pres.push(pre.bytes);
			}
		}

		let posts: Vec<EncodedBytes> =
			if matched.is_empty() || self.ringbuffer_row_pre_update_interceptors().is_empty() {
				matched.iter().map(|&idx| rows[idx].clone()).collect()
			} else {
				let mut builders: Vec<_> = matched
					.iter()
					.map(|&idx| EncodedRingBufferRow::from(rows[idx].clone()).thaw())
					.collect();
				RingBufferRowInterceptor::pre_update(self, ringbuffer, &updated_ids, &mut builders)?;
				builders.into_iter().map(|builder| builder.freeze_bytes()).collect()
			};

		for (&idx, bytes) in matched.iter().zip(&posts) {
			if let Some(expected) = partitions.get(idx).copied()
				&& partition_of(
					&ringbuffer.columns,
					&ringbuffer.partition_by,
					&partition_values(&shape, bytes, &indices),
				) != expected
			{
				return Err(PartitionError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}

			let key = &keys[idx];
			if self.get_committed(key)?.is_some() {
				self.mark_preexisting(key)?;
			}
			self.set(key, bytes.clone())?;
		}

		if !matched.is_empty() && !self.ringbuffer_row_post_update_interceptors().is_empty() {
			RingBufferRowInterceptor::post_update(self, ringbuffer, &updated_ids, &posts, &pres)?;
		}

		let mut stored: Vec<EncodedBytes> = rows.to_vec();
		for (&idx, bytes) in matched.iter().zip(&posts) {
			stored[idx] = bytes.clone();
		}

		if !updated_ids.is_empty() {
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				&shape,
				&updated_ids,
				&pres,
				&posts,
			)?);
		}

		Ok(stored)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
	) -> Result<Vec<EncodedBytes>> {
		let mut displayed_rows = Vec::with_capacity(ids.len());
		let mut removed_ids = Vec::new();
		let mut removed = Vec::new();
		let mut matched = Vec::new();

		for (idx, &id) in ids.iter().enumerate() {
			let key = ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id);

			let displayed = match self.get(&key)? {
				Some(v) => v.bytes,
				None => {
					displayed_rows.push(EncodedBytes(CowVec::new(vec![])));
					continue;
				}
			};
			let committed = self.get_committed(&key)?.map(|v| v.bytes);

			removed_ids.push(id);
			removed.push(committed.clone().unwrap_or_else(|| displayed.clone()));
			matched.push((key, committed.is_some()));
			displayed_rows.push(displayed);
		}

		if !removed_ids.is_empty() && !self.ringbuffer_row_pre_delete_interceptors().is_empty() {
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &removed_ids)?;
		}

		for ((key, committed), pre_for_cdc) in matched.iter().zip(&removed) {
			if *committed {
				self.mark_preexisting(key)?;
			}
			self.remove_with_pre(key, pre_for_cdc.clone())?;
		}

		if !removed_ids.is_empty() && !self.ringbuffer_row_post_delete_interceptors().is_empty() {
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &removed_ids, &removed)?;
		}

		if !removed_ids.is_empty() {
			let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
			self.track_flow_change(build_ringbuffer_remove_change(
				ringbuffer,
				&shape,
				&removed_ids,
				&removed,
			)?);
		}

		Ok(displayed_rows)
	}
}

impl RingBufferOperations for AdminTransaction {
	fn insert_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		let mut stored = Vec::with_capacity(rows.len());
		let mut inserted_ids = Vec::new();
		let mut inserted = Vec::new();
		let mut replaced_ids = Vec::new();
		let mut replaced_pres = Vec::new();
		let mut replaced_posts = Vec::new();
		if ids.is_empty() {
			return Ok(stored);
		}

		let keys: Vec<TaggedKey> = ids
			.iter()
			.enumerate()
			.map(|(idx, &row_number)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number))
			.collect();
		let mut pres = Vec::with_capacity(keys.len());
		for key in &keys {
			pres.push(self.get(key)?.map(|v| v.bytes));
		}

		for (&row_number, pre) in ids.iter().zip(&pres) {
			if let Some(pre) = pre {
				replaced_ids.push(row_number);
				replaced_pres.push(pre.clone());
			}
		}
		if !replaced_ids.is_empty() {
			if !self.ringbuffer_row_pre_delete_interceptors().is_empty() {
				RingBufferRowInterceptor::pre_delete(self, ringbuffer, &replaced_ids)?;
			}
			if !self.ringbuffer_row_post_delete_interceptors().is_empty() {
				RingBufferRowInterceptor::post_delete(self, ringbuffer, &replaced_ids, &replaced_pres)?;
			}
		}

		let written: Vec<EncodedBytes> = if self.ringbuffer_row_pre_insert_interceptors().is_empty() {
			rows.to_vec()
		} else {
			let mut builders: Vec<_> =
				rows.iter().map(|bytes| EncodedRingBufferRow::from(bytes.clone()).thaw()).collect();
			RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut builders)?;
			builders.into_iter().map(|builder| builder.freeze_bytes()).collect()
		};

		for (key, bytes) in keys.iter().zip(&written) {
			self.set(key, bytes.clone())?;
		}

		if !self.ringbuffer_row_post_insert_interceptors().is_empty() {
			RingBufferRowInterceptor::post_insert(self, ringbuffer, ids, &written)?;
		}

		for ((&row_number, pre), bytes) in ids.iter().zip(&pres).zip(written) {
			match pre {
				Some(_) => replaced_posts.push(bytes.clone()),
				None => {
					inserted_ids.push(row_number);
					inserted.push(bytes.clone());
				}
			}
			stored.push(bytes);
		}

		if !inserted_ids.is_empty() {
			self.track_flow_change(build_ringbuffer_insert_change(
				ringbuffer,
				shape,
				&inserted_ids,
				&inserted,
			)?);
		}
		if !replaced_ids.is_empty() {
			let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				&shape,
				&replaced_ids,
				&replaced_pres,
				&replaced_posts,
			)?);
		}

		Ok(stored)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
		let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
		let mut updated_ids = Vec::new();
		let mut pres = Vec::new();
		let mut matched = Vec::new();

		let keys: Vec<TaggedKey> = ids
			.iter()
			.enumerate()
			.map(|(idx, &id)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id))
			.collect();
		for (idx, key) in keys.iter().enumerate() {
			if let Some(pre) = self.get(key)? {
				matched.push(idx);
				updated_ids.push(ids[idx]);
				pres.push(pre.bytes);
			}
		}

		let posts: Vec<EncodedBytes> =
			if matched.is_empty() || self.ringbuffer_row_pre_update_interceptors().is_empty() {
				matched.iter().map(|&idx| rows[idx].clone()).collect()
			} else {
				let mut builders: Vec<_> = matched
					.iter()
					.map(|&idx| EncodedRingBufferRow::from(rows[idx].clone()).thaw())
					.collect();
				RingBufferRowInterceptor::pre_update(self, ringbuffer, &updated_ids, &mut builders)?;
				builders.into_iter().map(|builder| builder.freeze_bytes()).collect()
			};

		for (&idx, bytes) in matched.iter().zip(&posts) {
			if let Some(expected) = partitions.get(idx).copied()
				&& partition_of(
					&ringbuffer.columns,
					&ringbuffer.partition_by,
					&partition_values(&shape, bytes, &indices),
				) != expected
			{
				return Err(PartitionError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}

			let key = &keys[idx];
			if self.get_committed(key)?.is_some() {
				self.mark_preexisting(key)?;
			}
			self.set(key, bytes.clone())?;
		}

		if !matched.is_empty() && !self.ringbuffer_row_post_update_interceptors().is_empty() {
			RingBufferRowInterceptor::post_update(self, ringbuffer, &updated_ids, &posts, &pres)?;
		}

		let mut stored: Vec<EncodedBytes> = rows.to_vec();
		for (&idx, bytes) in matched.iter().zip(&posts) {
			stored[idx] = bytes.clone();
		}

		if !updated_ids.is_empty() {
			self.track_flow_change(build_ringbuffer_update_change(
				ringbuffer,
				&shape,
				&updated_ids,
				&pres,
				&posts,
			)?);
		}

		Ok(stored)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
	) -> Result<Vec<EncodedBytes>> {
		let mut displayed_rows = Vec::with_capacity(ids.len());
		let mut removed_ids = Vec::new();
		let mut removed = Vec::new();
		let mut matched = Vec::new();

		for (idx, &id) in ids.iter().enumerate() {
			let key = ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id);

			let displayed = match self.get(&key)? {
				Some(v) => v.bytes,
				None => {
					displayed_rows.push(EncodedBytes(CowVec::new(vec![])));
					continue;
				}
			};
			let committed = self.get_committed(&key)?.map(|v| v.bytes);

			removed_ids.push(id);
			removed.push(committed.clone().unwrap_or_else(|| displayed.clone()));
			matched.push((key, committed.is_some()));
			displayed_rows.push(displayed);
		}

		if !removed_ids.is_empty() && !self.ringbuffer_row_pre_delete_interceptors().is_empty() {
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &removed_ids)?;
		}

		for ((key, committed), pre_for_cdc) in matched.iter().zip(&removed) {
			if *committed {
				self.mark_preexisting(key)?;
			}
			self.remove_with_pre(key, pre_for_cdc.clone())?;
		}

		if !removed_ids.is_empty() && !self.ringbuffer_row_post_delete_interceptors().is_empty() {
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &removed_ids, &removed)?;
		}

		if !removed_ids.is_empty() {
			let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
			self.track_flow_change(build_ringbuffer_remove_change(
				ringbuffer,
				&shape,
				&removed_ids,
				&removed,
			)?);
		}

		Ok(displayed_rows)
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn insert_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => txn.insert_ringbuffer(ringbuffer, shape, partitions, ids, rows),
			Transaction::Admin(txn) => txn.insert_ringbuffer(ringbuffer, shape, partitions, ids, rows),
			Transaction::Test(t) => t.inner.insert_ringbuffer(ringbuffer, shape, partitions, ids, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, partitions, ids, rows),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, partitions, ids, rows),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, partitions, ids, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, partitions, ids),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
