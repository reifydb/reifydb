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
	key::any::TaggedKey,
	partition::{PartitionError, partition_col_indices, partitions_of_rows},
	ring::{Placement, Ring, oldest_first},
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_flow::writer::ring::{
	RingAdapter, RingCounters, RingDeleteRows, RingInsertRows, RingUpdateRows, RingWriter, ring_row_tagged,
};
use reifydb_transaction::{
	interceptor::ringbuffer_row::RingBufferRowInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::value::{datetime::DateTime, partition::Partition, row_number::RowNumber};
use smallvec::{SmallVec, smallvec};

use crate::{
	Result,
	transaction::operation::{storage::DmlTransaction, table::TableDml},
};

pub fn ringbuffer_key(ringbuffer: &RingBuffer, partition: Option<Partition>, row_number: RowNumber) -> TaggedKey {
	ring_row_tagged(ringbuffer.id.into(), partition, row_number)
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

struct CatalogRing<'a> {
	catalog: &'a Catalog,
	ringbuffer: &'a RingBuffer,
}

impl RingCounters<DmlTransaction<'_>> for CatalogRing<'_> {
	fn metadata(
		&self,
		txn: &mut DmlTransaction<'_>,
		partition: Option<Partition>,
	) -> Result<Option<RingBufferMetadata>> {
		self.catalog.find_partition_metadata(&mut txn.tx, self.ringbuffer, partition)
	}

	fn reserve(&self, txn: &mut DmlTransaction<'_>, capacity: u64) -> Result<u64> {
		Ok(self.catalog.reserve_ringbuffer_row_numbers(&mut txn.tx, self.ringbuffer.id, capacity)?.0)
	}

	fn save_metadata(
		&self,
		txn: &mut DmlTransaction<'_>,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		self.catalog.save_partition_metadata(&mut txn.tx, self.ringbuffer, partition, metadata)
	}

	fn remove_metadata(&self, txn: &mut DmlTransaction<'_>, partition: Option<Partition>) -> Result<()> {
		self.catalog.remove_partition_metadata(&mut txn.tx, self.ringbuffer, partition)
	}
}

pub fn place_ring_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	ring: &mut Ring,
	partition: Option<Partition>,
	count: usize,
) -> Result<Vec<Placement>> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingWriter::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into())
		.place(&counters, ring, partition, count)
}

pub fn release_ring_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	ring: &mut Ring,
	partition: Option<Partition>,
	rows: &[RowNumber],
) -> Result<Vec<Option<EncodedBytes>>> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingWriter::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into())
		.release(&counters, ring, partition, rows)
}

pub fn ring_rows_oldest_first(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	partition: Option<Partition>,
) -> Result<Vec<RowNumber>> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	oldest_first(
		&mut RingAdapter {
			txn: &mut DmlTransaction::over(txn.reborrow()),
			counters: &counters,
			storage: ringbuffer.id.into(),
		},
		partition,
	)
}

pub fn save_ring(catalog: &Catalog, txn: &mut Transaction<'_>, ringbuffer: &RingBuffer, ring: &Ring) -> Result<()> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingWriter::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into()).save(&counters, ring)
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

fn write_rows<T: TableDml>(
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

	let written = RingWriter::new(&mut DmlTransaction::over(txn.transaction()), ringbuffer.id.into())
		.insert(RingInsertRows::Tagged {
			rows: keys.into_iter().zip(written).collect(),
		})?
		.written;

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

fn update_rows<T: TableDml>(
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

	let posts = RingWriter::new(&mut DmlTransaction::over(txn.transaction()), ringbuffer.id.into())
		.update(RingUpdateRows::Tagged {
			rows: keys.into_iter().zip(posts).collect(),
		})?
		.written;

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

fn remove_rows<T: TableDml>(
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

	let removed = RingWriter::new(&mut DmlTransaction::over(txn.transaction()), ringbuffer.id.into())
		.delete(RingDeleteRows::WithPre {
			rows: keys.into_iter().zip(removed).collect(),
		})?
		.replaced;

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

#[cfg(test)]
mod hook_tests {
	use std::sync::{Arc, Mutex};

	use reifydb_core::{
		common::TimeSource,
		delta::{Delta, RemoveAnnounce, SetPre},
		interface::catalog::id::{NamespaceId, RingBufferId},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::interceptor::{
		WithInterceptors,
		ringbuffer_row::{
			ringbuffer_row_post_delete, ringbuffer_row_post_insert, ringbuffer_row_post_update,
			ringbuffer_row_pre_delete, ringbuffer_row_pre_insert, ringbuffer_row_pre_update,
		},
	};

	use super::*;

	type Log = Arc<Mutex<Vec<String>>>;

	fn ringbuffer() -> RingBuffer {
		RingBuffer {
			id: RingBufferId(1),
			namespace: NamespaceId(1),
			name: "r".to_string(),
			columns: vec![],
			capacity: 2,
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::None,
		}
	}

	fn row(sequence: u64) -> EncodedBytes {
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		let mut builder = shape.allocate_ringbuffer();
		builder.set_sequence(sequence);
		builder.freeze_bytes()
	}

	fn sequence_of(row: &EncodedBytes) -> u64 {
		EncodedRingBufferRow::view(row).sequence()
	}

	fn hooked(txn: &mut AdminTransaction) -> Log {
		let log: Log = Arc::new(Mutex::new(Vec::new()));
		let entry = |log: &Log| log.clone();
		let at = entry(&log);
		txn.ringbuffer_row_pre_delete_interceptors().add(Arc::new(ringbuffer_row_pre_delete(move |ctx| {
			at.lock().unwrap().push(format!("pre_delete {:?}", ctx.ids));
			Ok(())
		})));
		let at = entry(&log);
		txn.ringbuffer_row_post_delete_interceptors().add(Arc::new(ringbuffer_row_post_delete(move |ctx| {
			let rows: Vec<u64> = ctx.deleted_rows.iter().map(sequence_of).collect();
			at.lock().unwrap().push(format!("post_delete {rows:?}"));
			Ok(())
		})));
		let at = entry(&log);
		txn.ringbuffer_row_pre_insert_interceptors().add(Arc::new(ringbuffer_row_pre_insert(move |_| {
			at.lock().unwrap().push("pre_insert".to_string());
			Ok(())
		})));
		let at = entry(&log);
		txn.ringbuffer_row_post_insert_interceptors().add(Arc::new(ringbuffer_row_post_insert(move |ctx| {
			let rows: Vec<u64> = ctx.rows.iter().map(sequence_of).collect();
			at.lock().unwrap().push(format!("post_insert {rows:?}"));
			Ok(())
		})));
		let at = entry(&log);
		txn.ringbuffer_row_pre_update_interceptors().add(Arc::new(ringbuffer_row_pre_update(move |_| {
			at.lock().unwrap().push("pre_update".to_string());
			Ok(())
		})));
		let at = entry(&log);
		txn.ringbuffer_row_post_update_interceptors().add(Arc::new(ringbuffer_row_post_update(move |ctx| {
			let posts: Vec<u64> = ctx.posts.iter().map(sequence_of).collect();
			let pres: Vec<u64> = ctx.pres.iter().map(sequence_of).collect();
			at.lock().unwrap().push(format!("post_update {posts:?} {pres:?}"));
			Ok(())
		})));
		log
	}

	fn pending(txn: &AdminTransaction, key: &TaggedKey) -> Delta {
		txn.cmd.as_ref().unwrap().pending_writes().get(key).unwrap().delta.clone()
	}

	#[test]
	fn evicted_delete_hooks_fire_with_the_evicted_row_before_insert_hooks() {
		// Delete hooks after insert hooks would show a consumer the new row while the old one still counts.
		let mut txn = create_test_admin_transaction();
		let log = hooked(&mut txn);
		let ids = [RowNumber(1), RowNumber(2)];
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		let written = txn
			.write_ringbuffer(&ringbuffer(), &shape, &[], &ids, &[row(5), row(6)], &[Some(row(3)), None])
			.unwrap();
		assert_eq!(written, vec![row(5), row(6)]);
		assert_eq!(
			*log.lock().unwrap(),
			vec![
				"pre_delete [RowNumber(1)]".to_string(),
				"post_delete [3]".to_string(),
				"pre_insert".to_string(),
				"post_insert [5, 6]".to_string(),
			]
		);
		let key = ringbuffer_key(&ringbuffer(), None, RowNumber(1));
		assert!(matches!(
			pending(&txn, &key),
			Delta::Set {
				pre: SetPre::Unknown,
				..
			}
		));
	}

	#[test]
	fn an_insert_without_evictions_fires_no_delete_hook() {
		// A delete hook for a fresh slot would report a row that never existed.
		let mut txn = create_test_admin_transaction();
		let log = hooked(&mut txn);
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		txn.write_ringbuffer(&ringbuffer(), &shape, &[], &[RowNumber(1)], &[row(5)], &[None]).unwrap();
		assert_eq!(*log.lock().unwrap(), vec!["pre_insert".to_string(), "post_insert [5]".to_string()]);
	}

	#[test]
	fn an_update_keeps_the_callers_pre_for_its_hook_and_tells_the_store_none() {
		// Ring update DML gives the store no pre today; passing one would change the CDC pre-image source.
		let mut txn = create_test_admin_transaction();
		let log = hooked(&mut txn);
		let stored = txn.update_ringbuffer(&ringbuffer(), &[], &[RowNumber(1)], &[row(7)], &[row(4)]).unwrap();
		assert_eq!(stored, vec![row(7)]);
		assert_eq!(*log.lock().unwrap(), vec!["pre_update".to_string(), "post_update [7] [4]".to_string()]);
		let key = ringbuffer_key(&ringbuffer(), None, RowNumber(1));
		assert!(matches!(
			pending(&txn, &key),
			Delta::Set {
				pre: SetPre::Unknown,
				..
			}
		));
	}

	#[test]
	fn a_remove_hands_the_displayed_pre_to_the_store_and_the_post_delete_hook() {
		// With nothing committed the displayed row is the pre-image; dropping it would lose the CDC remove.
		let mut txn = create_test_admin_transaction();
		let log = hooked(&mut txn);
		txn.remove_from_ringbuffer(&ringbuffer(), &[], &[RowNumber(1)], &[row(4)]).unwrap();
		assert_eq!(
			*log.lock().unwrap(),
			vec!["pre_delete [RowNumber(1)]".to_string(), "post_delete [4]".to_string()]
		);
		let key = ringbuffer_key(&ringbuffer(), None, RowNumber(1));
		assert!(matches!(
			pending(&txn, &key),
			Delta::Remove { announce: RemoveAnnounce::Announced { pre }, .. } if pre == row(4)
		));
	}
}

#[cfg(test)]
mod free_stack_tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			id::{NamespaceId, RingBufferId},
			ringbuffer::{encode_ringbuffer_free_page, encode_ringbuffer_order},
		},
		key::ringbuffer::{RingBufferFreeKey, ring_order_key},
		ring::FREE_PAGE_SLOTS,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;

	use super::*;
	use crate::vm::executor::Executor;

	const CAPACITY: u64 = 1_000_000;
	const BASE: u64 = 1_000;

	#[test]
	fn emptying_a_million_slot_partition_removes_every_page_under_the_write_limit() {
		// One free key per slot would fail this delete with too many writes, every time it is retried.
		let partition = Some(Partition(1));
		let ringbuffer = RingBuffer {
			id: RingBufferId(1),
			namespace: NamespaceId(1),
			name: "r".to_string(),
			columns: vec![],
			capacity: CAPACITY,
			primary_key: None,
			partition_by: vec!["p".to_string()],
			time: TimeSource::None,
		};
		let executor = Executor::testing();
		let mut admin = create_test_admin_transaction();
		let mut txn = Transaction::Admin(&mut admin);
		let storage = ringbuffer.id;
		let free = CAPACITY - 1;
		let last_row = RowNumber(BASE + CAPACITY - 1);
		let sequence = CAPACITY - 1;
		executor.catalog
			.save_partition_metadata(
				&mut txn,
				&ringbuffer,
				partition,
				&RingBufferMetadata {
					base_rn: BASE,
					next_sequence: CAPACITY,
					count: 1,
					filled: CAPACITY,
					head: sequence,
				},
			)
			.unwrap();
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);
		let mut row = shape.allocate_ringbuffer();
		row.set_sequence(sequence);
		txn.set(vec![(ringbuffer_key(&ringbuffer, partition, last_row), row.freeze_bytes())]).unwrap();
		txn.set(vec![(
			ring_order_key(storage, partition, sequence),
			encode_ringbuffer_order(last_row).into_bytes(),
		)])
		.unwrap();
		let full_page = vec![RowNumber(BASE); FREE_PAGE_SLOTS as usize];
		let pages = free.div_ceil(FREE_PAGE_SLOTS);
		for page in 0..pages {
			let slots = (free - page * FREE_PAGE_SLOTS).min(FREE_PAGE_SLOTS) as usize;
			txn.set(vec![(
				RingBufferFreeKey::of(storage, partition, page).into(),
				encode_ringbuffer_free_page(&full_page[..slots]).into_bytes(),
			)])
			.unwrap();
		}

		let mut ring = Ring::new(CAPACITY);
		let removed =
			release_ring_rows(&executor.catalog, &mut txn, &ringbuffer, &mut ring, partition, &[last_row])
				.unwrap();
		assert!(removed[0].is_some());
		save_ring(&executor.catalog, &mut txn, &ringbuffer, &ring).unwrap();

		let left = txn
			.range(RingBufferFreeKey::full_scan_for_storage(storage), RangeScope::All, 1024)
			.unwrap()
			.count();
		assert_eq!(left, 0, "every free page of the emptied partition must go");
		assert_eq!(executor.catalog.find_partition_metadata(&mut txn, &ringbuffer, partition).unwrap(), None);
		assert_eq!(pages, 1954);
		assert!(
			admin.cmd.as_ref().unwrap().pending_writes().len() < 2_000,
			"the whole commit must stay far under the one million write limit"
		);
	}

	#[test]
	fn reuse_through_a_real_transaction_is_last_freed_first_across_sessions_and_a_page_boundary() {
		// Row numbers are user visible, so a page read or written under the wrong key changes query results.
		let capacity = FREE_PAGE_SLOTS + 3;
		let ringbuffer = RingBuffer {
			id: RingBufferId(1),
			namespace: NamespaceId(1),
			name: "r".to_string(),
			columns: vec![],
			capacity,
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::None,
		};
		let executor = Executor::testing();
		let catalog = &executor.catalog;
		let mut admin = create_test_admin_transaction();
		let mut txn = Transaction::Admin(&mut admin);
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer.columns);

		let mut ring = Ring::new(capacity);
		let placed =
			place_ring_rows(catalog, &mut txn, &ringbuffer, &mut ring, None, capacity as usize).unwrap();
		for placement in &placed {
			let mut row = shape.allocate_ringbuffer();
			row.set_sequence(placement.sequence);
			txn.set(vec![(ringbuffer_key(&ringbuffer, None, placement.row), row.freeze_bytes())]).unwrap();
		}
		save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();
		let rows: Vec<RowNumber> = placed.iter().map(|placement| placement.row).collect();

		let mut ring = Ring::new(capacity);
		release_ring_rows(
			catalog,
			&mut txn,
			&ringbuffer,
			&mut ring,
			None,
			&rows[..FREE_PAGE_SLOTS as usize + 2],
		)
		.unwrap();
		save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();

		for expected in [
			rows[FREE_PAGE_SLOTS as usize + 1],
			rows[FREE_PAGE_SLOTS as usize],
			rows[FREE_PAGE_SLOTS as usize - 1],
		] {
			let mut ring = Ring::new(capacity);
			let reused = place_ring_rows(catalog, &mut txn, &ringbuffer, &mut ring, None, 1).unwrap();
			assert_eq!(reused[0].row, expected);
			assert_eq!(reused[0].evicted, None);
			save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();
		}
	}
}
