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
	ring::{MetadataWrite, Ring, RingStore},
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_transaction::{
	interceptor::{WithInterceptors, ringbuffer_row::RingBufferRowInterceptor},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction, read::Read, write::Write},
};
use reifydb_value::value::{datetime::DateTime, partition::Partition, row_number::RowNumber};
use smallvec::{SmallVec, smallvec};

use crate::Result;

pub fn ringbuffer_key(ringbuffer: &RingBuffer, partition: Option<Partition>, row_number: RowNumber) -> TaggedKey {
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

pub struct RingTxn<'a, 'b, 't> {
	pub catalog: &'a Catalog,
	pub txn: &'a mut Transaction<'t>,
	pub ringbuffer: &'b RingBuffer,
}

impl RingStore for RingTxn<'_, '_, '_> {
	fn metadata(&mut self, partition: Option<Partition>) -> Result<Option<RingBufferMetadata>> {
		self.catalog.find_partition_metadata(self.txn, self.ringbuffer, partition)
	}

	fn reserve(&mut self, capacity: u64) -> Result<u64> {
		Ok(self.catalog.reserve_ringbuffer_row_numbers(self.txn, self.ringbuffer.id, capacity)?.0)
	}

	fn slots(&mut self, partition: Option<Partition>, rows: &[RowNumber]) -> Result<Vec<Option<EncodedBytes>>> {
		if rows.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<TaggedKey> =
			rows.iter().map(|&row| ringbuffer_key(self.ringbuffer, partition, row)).collect();
		Ok(self.txn.get(&keys)?.into_iter().map(|row| row.map(|row| row.bytes)).collect())
	}
}

pub fn save_ring(catalog: &Catalog, txn: &mut Transaction<'_>, ringbuffer: &RingBuffer, ring: &Ring) -> Result<()> {
	for write in ring.writes() {
		match write {
			MetadataWrite::Save(partition, metadata) => {
				catalog.save_partition_metadata(txn, ringbuffer, partition, &metadata)?
			}
			MetadataWrite::Remove(partition) => {
				catalog.remove_partition_metadata(txn, ringbuffer, partition)?
			}
		}
	}
	Ok(())
}

fn ringbuffer_change(ringbuffer: &RingBuffer, diffs: SmallVec<[Diff; 4]>) -> Change {
	Change {
		origin: ChangeOrigin::Object(ObjectId::ringbuffer(ringbuffer.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs,
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_write_change(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	ids: &[RowNumber],
	rows: &[EncodedBytes],
	evicted: &[Option<EncodedBytes>],
) -> Result<Change> {
	let mut diffs: SmallVec<[Diff; 4]> = SmallVec::new();
	let fresh: Vec<usize> = (0..ids.len()).filter(|&idx| evicted[idx].is_none()).collect();
	if !fresh.is_empty() {
		let fresh_ids: Vec<RowNumber> = fresh.iter().map(|&idx| ids[idx]).collect();
		let fresh_rows: Vec<EncodedBytes> = fresh.iter().map(|&idx| rows[idx].clone()).collect();
		diffs.push(Diff::insert(from_encoded_bytes(shape, &fresh_ids, &fresh_rows)?));
	}
	if fresh.len() < ids.len() {
		let mut over_ids = Vec::new();
		let mut pres = Vec::new();
		let mut posts = Vec::new();
		for (idx, pre) in evicted.iter().enumerate() {
			if let Some(pre) = pre {
				over_ids.push(ids[idx]);
				pres.push(pre.clone());
				posts.push(rows[idx].clone());
			}
		}
		diffs.push(Diff::update(
			from_encoded_bytes(shape, &over_ids, &pres)?,
			from_encoded_bytes(shape, &over_ids, &posts)?,
		));
	}
	Ok(ringbuffer_change(ringbuffer, diffs))
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
		smallvec![Diff::update(from_encoded_bytes(shape, ids, pres)?, from_encoded_bytes(shape, ids, posts)?)],
	))
}

fn build_ringbuffer_remove_change(
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	ids: &[RowNumber],
	rows: &[EncodedBytes],
) -> Result<Change> {
	Ok(ringbuffer_change(ringbuffer, smallvec![Diff::remove(from_encoded_bytes(shape, ids, rows)?)]))
}

pub trait RingBufferOperations {
	fn write_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		evicted: &[Option<EncodedBytes>],
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
		displayed: &[EncodedBytes],
	) -> Result<()>;
}

fn write_rows<T: WithInterceptors + Write + Read>(
	txn: &mut T,
	ringbuffer: &RingBuffer,
	shape: &RowShape,
	partitions: &[Partition],
	ids: &[RowNumber],
	rows: &[EncodedBytes],
	evicted: &[Option<EncodedBytes>],
) -> Result<Vec<EncodedBytes>> {
	assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
	assert_eq!(ids.len(), evicted.len(), "ids/evicted length mismatch");
	if ids.is_empty() {
		return Ok(Vec::new());
	}

	let keys: Vec<TaggedKey> = ids
		.iter()
		.enumerate()
		.map(|(idx, &row_number)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), row_number))
		.collect();

	let (evicted_ids, evicted_rows): (Vec<RowNumber>, Vec<EncodedBytes>) =
		ids.iter().zip(evicted).filter_map(|(&id, pre)| pre.as_ref().map(|pre| (id, pre.clone()))).unzip();
	if !evicted_ids.is_empty() && !txn.ringbuffer_row_pre_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::pre_delete(txn, ringbuffer, &evicted_ids)?;
	}
	if !evicted_ids.is_empty() && !txn.ringbuffer_row_post_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::post_delete(txn, ringbuffer, &evicted_ids, &evicted_rows)?;
	}

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

	txn.set_with_pre(
		keys.into_iter()
			.zip(written.iter().cloned())
			.map(|(key, bytes)| (key, bytes, SetPre::Unknown))
			.collect(),
	)?;

	if !txn.ringbuffer_row_post_insert_interceptors().is_empty() {
		RingBufferRowInterceptor::post_insert(txn, ringbuffer, ids, &written)?;
	}

	let object = ObjectId::ringbuffer(ringbuffer.id);
	txn.touch_flow_object(object);
	if txn.wants_flow_diff(object) {
		txn.track_flow_change(build_ringbuffer_write_change(ringbuffer, shape, ids, &written, evicted)?);
	}

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

	txn.set(keys.into_iter().zip(posts.iter().cloned()).collect())?;

	if !txn.ringbuffer_row_post_update_interceptors().is_empty() {
		RingBufferRowInterceptor::post_update(txn, ringbuffer, ids, &posts, pres)?;
	}

	let object = ObjectId::ringbuffer(ringbuffer.id);
	txn.touch_flow_object(object);
	if txn.wants_flow_diff(object) {
		txn.track_flow_change(build_ringbuffer_update_change(ringbuffer, &shape, ids, pres, &posts)?);
	}

	Ok(posts)
}

fn remove_rows<T: WithInterceptors + Write + Read>(
	txn: &mut T,
	ringbuffer: &RingBuffer,
	partitions: &[Partition],
	ids: &[RowNumber],
	displayed: &[EncodedBytes],
) -> Result<()> {
	assert_eq!(ids.len(), displayed.len(), "ids/displayed length mismatch");
	if ids.is_empty() {
		return Ok(());
	}
	let keys: Vec<TaggedKey> = ids
		.iter()
		.enumerate()
		.map(|(idx, &id)| ringbuffer_key(ringbuffer, partitions.get(idx).copied(), id))
		.collect();

	let committed: Vec<Option<EncodedBytes>> =
		txn.get_committed(&keys)?.into_iter().map(|row| row.map(|v| v.bytes)).collect();
	let removed: Vec<EncodedBytes> = committed
		.iter()
		.zip(displayed)
		.map(|(committed, displayed)| committed.clone().unwrap_or_else(|| displayed.clone()))
		.collect();

	if !txn.ringbuffer_row_pre_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::pre_delete(txn, ringbuffer, ids)?;
	}

	txn.remove(keys.into_iter().zip(removed.iter().cloned().map(Some)).collect())?;

	if !txn.ringbuffer_row_post_delete_interceptors().is_empty() {
		RingBufferRowInterceptor::post_delete(txn, ringbuffer, ids, &removed)?;
	}

	let object = ObjectId::ringbuffer(ringbuffer.id);
	txn.touch_flow_object(object);
	if txn.wants_flow_diff(object) {
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
		txn.track_flow_change(build_ringbuffer_remove_change(ringbuffer, &shape, ids, &removed)?);
	}

	Ok(())
}

impl RingBufferOperations for CommandTransaction {
	fn write_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		evicted: &[Option<EncodedBytes>],
	) -> Result<Vec<EncodedBytes>> {
		write_rows(self, ringbuffer, shape, partitions, ids, rows, evicted)
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
		displayed: &[EncodedBytes],
	) -> Result<()> {
		remove_rows(self, ringbuffer, partitions, ids, displayed)
	}
}

impl RingBufferOperations for AdminTransaction {
	fn write_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		evicted: &[Option<EncodedBytes>],
	) -> Result<Vec<EncodedBytes>> {
		write_rows(self, ringbuffer, shape, partitions, ids, rows, evicted)
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
		displayed: &[EncodedBytes],
	) -> Result<()> {
		remove_rows(self, ringbuffer, partitions, ids, displayed)
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn write_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partitions: &[Partition],
		ids: &[RowNumber],
		rows: &[EncodedBytes],
		evicted: &[Option<EncodedBytes>],
	) -> Result<Vec<EncodedBytes>> {
		match self {
			Transaction::Command(txn) => {
				txn.write_ringbuffer(ringbuffer, shape, partitions, ids, rows, evicted)
			}
			Transaction::Admin(txn) => {
				txn.write_ringbuffer(ringbuffer, shape, partitions, ids, rows, evicted)
			}
			Transaction::Test(t) => {
				t.inner.write_ringbuffer(ringbuffer, shape, partitions, ids, rows, evicted)
			}
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
		displayed: &[EncodedBytes],
	) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, partitions, ids, displayed),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
