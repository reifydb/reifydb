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
	delta::SetPre,
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
	partition::{PartitionError, partition_col_indices, partitions_of_rows},
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_transaction::{
	interceptor::{WithInterceptors, ringbuffer_row::RingBufferRowInterceptor},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction, read::Read, write::Write},
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::Result;

pub(crate) fn ringbuffer_key(
	ringbuffer: &RingBuffer,
	partition: Option<Partition>,
	row_number: RowNumber,
) -> TaggedKey {
	match partition {
		None => RowKey::new(ringbuffer.id, row_number).into(),
		Some(partition) => PartitionedRowKey::new(ringbuffer.id, partition, row_number).into(),
	}
}

fn moved_partitions(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	partitions: &[Partition],
	rows: &[EncodedBytes],
) -> Result<bool> {
	let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
	Ok(!indices.is_empty() && partitions_of_rows(shape, &indices, rows)? != partitions)
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
		pres: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>>;

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		displayed: Vec<Option<EncodedBytes>>,
	) -> Result<Vec<EncodedBytes>>;
}

fn insert_rows<T: WithInterceptors + Write + Read>(
	txn: &mut T,
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	partitions: &[Partition],
	ids: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<Vec<EncodedBytes>> {
	assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
	if ids.is_empty() {
		return Ok(Vec::new());
	}

	let keys: Vec<TaggedKey> = ids
		.iter()
		.enumerate()
		.map(|(idx, &row_number)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number))
		.collect();

	let written: Vec<EncodedBytes> = if txn.ringbuffer_row_pre_insert_interceptors().is_empty() {
		rows.to_vec()
	} else {
		let mut builders: Vec<_> =
			rows.iter().map(|bytes| EncodedRingBufferRow::from(bytes.clone()).thaw()).collect();
		RingBufferRowInterceptor::pre_insert(txn, ringbuffer, &mut builders)?;
		let written: Vec<EncodedBytes> = builders.into_iter().map(|builder| builder.freeze_bytes()).collect();
		if moved_partitions(ringbuffer, shape, partitions, &written)? {
			return Err(PartitionError::PartitionColumnChangedByHook {
				object: ObjectId::ringbuffer(ringbuffer.id),
			}
			.into());
		}
		written
	};

	txn.set_many_with_pre(
		keys.into_iter()
			.zip(written.iter().cloned())
			.map(|(key, bytes)| (key, bytes, SetPre::Absent))
			.collect(),
	)?;

	if !txn.ringbuffer_row_post_insert_interceptors().is_empty() {
		RingBufferRowInterceptor::post_insert(txn, ringbuffer, ids, &written)?;
	}

	txn.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, ids, &written)?);

	Ok(written)
}

fn update_rows<T: WithInterceptors + Write + Read>(
	txn: &mut T,
	ringbuffer: &RingBuffer,
	partitions: &[Partition],
	ids: &[RowNumber],
	rows: &[EncodedBytes],
	pres: &[EncodedBytes],
) -> Result<Vec<EncodedBytes>> {
	assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
	assert_eq!(ids.len(), pres.len(), "ids/pres length mismatch");
	let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);

	let keys: Vec<TaggedKey> = ids
		.iter()
		.enumerate()
		.map(|(idx, &id)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id))
		.collect();

	let posts: Vec<EncodedBytes> = if txn.ringbuffer_row_pre_update_interceptors().is_empty() {
		rows.to_vec()
	} else {
		let mut builders: Vec<_> =
			rows.iter().map(|bytes| EncodedRingBufferRow::from(bytes.clone()).thaw()).collect();
		RingBufferRowInterceptor::pre_update(txn, ringbuffer, ids, &mut builders)?;
		let posts: Vec<EncodedBytes> = builders.into_iter().map(|builder| builder.freeze_bytes()).collect();
		if moved_partitions(ringbuffer, &shape, partitions, &posts)? {
			return Err(PartitionError::ImmutablePartitionColumn {
				object: ObjectId::ringbuffer(ringbuffer.id),
			}
			.into());
		}
		posts
	};

	txn.set_many(keys.into_iter().zip(posts.iter().cloned()).collect())?;

	if !txn.ringbuffer_row_post_update_interceptors().is_empty() {
		RingBufferRowInterceptor::post_update(txn, ringbuffer, ids, &posts, pres)?;
	}

	txn.track_flow_change(build_ringbuffer_update_change(ringbuffer, &shape, ids, pres, &posts)?);

	Ok(posts)
}

fn remove_rows<T: WithInterceptors + Write + Read>(
	txn: &mut T,
	ringbuffer: &RingBuffer,
	partitions: &[Partition],
	ids: &[RowNumber],
	displayed: Vec<Option<EncodedBytes>>,
) -> Result<Vec<EncodedBytes>> {
	assert_eq!(ids.len(), displayed.len(), "ids/displayed length mismatch");
	let mut displayed_rows = Vec::with_capacity(ids.len());
	let mut removed_ids = Vec::new();
	let mut matched_keys = Vec::new();
	let mut matched_displayed = Vec::new();

	for (idx, (&id, displayed)) in ids.iter().zip(displayed).enumerate() {
		let Some(displayed) = displayed else {
			displayed_rows.push(EncodedBytes(CowVec::new(vec![])));
			continue;
		};

		removed_ids.push(id);
		matched_keys.push(ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id));
		matched_displayed.push(displayed.clone());
		displayed_rows.push(displayed);
	}
	if removed_ids.is_empty() {
		return Ok(displayed_rows);
	}

	let committed: Vec<Option<EncodedBytes>> =
		txn.get_committed_many(&matched_keys)?.into_iter().map(|row| row.map(|v| v.bytes)).collect();
	let removed: Vec<EncodedBytes> = committed
		.iter()
		.zip(&matched_displayed)
		.map(|(committed, displayed)| committed.clone().unwrap_or_else(|| displayed.clone()))
		.collect();

	if !txn.ringbuffer_row_pre_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::pre_delete(txn, ringbuffer, &removed_ids)?;
	}

	txn.remove_many(matched_keys.into_iter().zip(removed.iter().cloned().map(Some)).collect())?;

	if !txn.ringbuffer_row_post_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::post_delete(txn, ringbuffer, &removed_ids, &removed)?;
	}

	let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
	txn.track_flow_change(build_ringbuffer_remove_change(ringbuffer, &shape, &removed_ids, &removed)?);

	Ok(displayed_rows)
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
		insert_rows(self, ringbuffer, shape, partitions, ids, rows)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		pres: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		update_rows(self, ringbuffer, partitions, ids, rows, pres)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		displayed: Vec<Option<EncodedBytes>>,
	) -> Result<Vec<EncodedBytes>> {
		remove_rows(self, ringbuffer, partitions, ids, displayed)
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
		insert_rows(self, ringbuffer, shape, partitions, ids, rows)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		pres: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		update_rows(self, ringbuffer, partitions, ids, rows, pres)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		displayed: Vec<Option<EncodedBytes>>,
	) -> Result<Vec<EncodedBytes>> {
		remove_rows(self, ringbuffer, partitions, ids, displayed)
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
		pres: &[EncodedBytes],
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, partitions, ids, rows, pres),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, partitions, ids, rows, pres),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, partitions, ids, rows, pres),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partitions: &[Partition],
		ids: &[RowNumber],
		displayed: Vec<Option<EncodedBytes>>,
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
