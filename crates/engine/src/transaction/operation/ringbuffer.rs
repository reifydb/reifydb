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
	interceptor::ringbuffer_row::RingBufferRowInterceptor,
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

		for (idx, (&row_number, bytes)) in ids.iter().zip(rows).enumerate() {
			let key = ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number);

			let pre = self.get(&key)?.map(|v| v.bytes);

			if let Some(ref existing) = pre {
				let ids = [row_number];
				let existing_rows = [existing.clone()];
				RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
				RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
			}

			let mut rows_buf = [EncodedRingBufferRow::from(bytes.clone()).thaw()];
			RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
			let [bytes] = rows_buf;
			let bytes = bytes.freeze_bytes();

			self.set(&key, bytes.clone())?;

			let row_ids = [row_number];
			let post_rows = [bytes.clone()];
			RingBufferRowInterceptor::post_insert(self, ringbuffer, &row_ids, &post_rows)?;

			match pre {
				Some(pre) => {
					replaced_ids.push(row_number);
					replaced_pres.push(pre);
					replaced_posts.push(bytes.clone());
				}
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
		let mut stored = Vec::with_capacity(rows.len());
		let mut updated_ids = Vec::new();
		let mut pres = Vec::new();
		let mut posts = Vec::new();

		for (idx, (&id, bytes)) in ids.iter().zip(rows).enumerate() {
			let partition = partitions.get(idx).copied();
			let key = ringbuffer_key(ringbuffer, partition, id);

			let pre = match self.get(&key)? {
				Some(v) => v.bytes,
				None => {
					stored.push(bytes.clone());
					continue;
				}
			};

			let mut rows_buf = [EncodedRingBufferRow::from(bytes.clone()).thaw()];
			let row_ids = [id];
			RingBufferRowInterceptor::pre_update(self, ringbuffer, &row_ids, &mut rows_buf)?;
			let [bytes] = rows_buf;
			let bytes = bytes.freeze_bytes();

			if let Some(expected) = partition
				&& partition_of(
					&ringbuffer.columns,
					&ringbuffer.partition_by,
					&partition_values(&shape, &bytes, &indices),
				) != expected
			{
				return Err(PartitionError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}

			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, bytes.clone())?;

			let post_rows = [bytes.clone()];
			let pre_rows = [pre.clone()];
			RingBufferRowInterceptor::post_update(self, ringbuffer, &row_ids, &post_rows, &pre_rows)?;

			updated_ids.push(id);
			pres.push(pre);
			posts.push(bytes.clone());
			stored.push(bytes);
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

			let row_ids = [id];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &row_ids)?;

			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

			if committed.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.remove_with_pre(&key, pre_for_cdc.clone())?;

			let pre_rows = [pre_for_cdc.clone()];
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &row_ids, &pre_rows)?;

			removed_ids.push(id);
			removed.push(pre_for_cdc);
			displayed_rows.push(displayed);
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

		for (idx, (&row_number, bytes)) in ids.iter().zip(rows).enumerate() {
			let key = ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number);

			let pre = self.get(&key)?.map(|v| v.bytes);

			if let Some(ref existing) = pre {
				let ids = [row_number];
				let existing_rows = [existing.clone()];
				RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
				RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
			}

			let mut rows_buf = [EncodedRingBufferRow::from(bytes.clone()).thaw()];
			RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
			let [bytes] = rows_buf;
			let bytes = bytes.freeze_bytes();

			self.set(&key, bytes.clone())?;

			let row_ids = [row_number];
			let post_rows = [bytes.clone()];
			RingBufferRowInterceptor::post_insert(self, ringbuffer, &row_ids, &post_rows)?;

			match pre {
				Some(pre) => {
					replaced_ids.push(row_number);
					replaced_pres.push(pre);
					replaced_posts.push(bytes.clone());
				}
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
		let mut stored = Vec::with_capacity(rows.len());
		let mut updated_ids = Vec::new();
		let mut pres = Vec::new();
		let mut posts = Vec::new();

		for (idx, (&id, bytes)) in ids.iter().zip(rows).enumerate() {
			let partition = partitions.get(idx).copied();
			let key = ringbuffer_key(ringbuffer, partition, id);

			let pre = match self.get(&key)? {
				Some(v) => v.bytes,
				None => {
					stored.push(bytes.clone());
					continue;
				}
			};

			let mut rows_buf = [EncodedRingBufferRow::from(bytes.clone()).thaw()];
			let row_ids = [id];
			RingBufferRowInterceptor::pre_update(self, ringbuffer, &row_ids, &mut rows_buf)?;
			let [bytes] = rows_buf;
			let bytes = bytes.freeze_bytes();

			if let Some(expected) = partition
				&& partition_of(
					&ringbuffer.columns,
					&ringbuffer.partition_by,
					&partition_values(&shape, &bytes, &indices),
				) != expected
			{
				return Err(PartitionError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}

			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, bytes.clone())?;

			let post_rows = [bytes.clone()];
			let pre_rows = [pre.clone()];
			RingBufferRowInterceptor::post_update(self, ringbuffer, &row_ids, &post_rows, &pre_rows)?;

			updated_ids.push(id);
			pres.push(pre);
			posts.push(bytes.clone());
			stored.push(bytes);
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

			let row_ids = [id];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &row_ids)?;

			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

			if committed.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.remove_with_pre(&key, pre_for_cdc.clone())?;

			let pre_rows = [pre_for_cdc.clone()];
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &row_ids, &pre_rows)?;

			removed_ids.push(id);
			removed.push(pre_for_cdc);
			displayed_rows.push(displayed);
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
