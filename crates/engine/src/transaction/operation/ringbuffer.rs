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
	key::any::TaggedKey,
	partition::{PartitionError, partition_col_indices, partitions_of_rows},
	ring::{Evicted, Placement, Ring, oldest_first},
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_flow::writer::ring::{RingAdapter, RingCounters, RingMetadata, RingWriter, ring_row_tagged};
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
		partitions: &[Option<Partition>],
	) -> Result<Vec<Option<RingBufferMetadata>>> {
		self.catalog.find_partition_metadata(&mut txn.tx, self.ringbuffer, partitions)
	}

	fn reserve(&self, txn: &mut DmlTransaction<'_>, batch_count: u64) -> Result<u64> {
		Ok(self.catalog.reserve_ringbuffer_row_numbers(&mut txn.tx, self.ringbuffer.id, batch_count)?.0)
	}

	fn save_metadata(
		&self,
		txn: &mut DmlTransaction<'_>,
		entries: &[(Option<Partition>, RingBufferMetadata)],
	) -> Result<()> {
		self.catalog.save_partition_metadata(&mut txn.tx, self.ringbuffer, entries)
	}

	fn remove_metadata(&self, txn: &mut DmlTransaction<'_>, partitions: &[Option<Partition>]) -> Result<()> {
		self.catalog.remove_partition_metadata(&mut txn.tx, self.ringbuffer, partitions)
	}
}

pub fn place_ring_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	ring: &mut Ring,
	requests: &[(Option<Partition>, Vec<RowNumber>)],
) -> Result<Vec<Vec<Placement>>> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingMetadata::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into())
		.place(&counters, ring, requests)
}

pub fn reserve_ring_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	count: usize,
) -> Result<u64> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingMetadata::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into()).reserve(&counters, count)
}

pub fn release_ring_rows(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	ring: &mut Ring,
	requests: &[(Option<Partition>, Vec<RowNumber>)],
) -> Result<Vec<Vec<Option<EncodedBytes>>>> {
	let counters = CatalogRing {
		catalog,
		ringbuffer,
	};
	RingMetadata::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into())
		.release(&counters, ring, requests)
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
	RingMetadata::new(&mut DmlTransaction::over(txn.reborrow()), ringbuffer.id.into()).save(&counters, ring)
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
	evicted: &[Option<Evicted>],
) -> Result<Change> {
	let mut diffs: SmallVec<[Diff; 4]> = SmallVec::new();
	let (victim_ids, victim_rows): (Vec<RowNumber>, Vec<EncodedBytes>) =
		evicted.iter().flatten().map(|victim| (victim.row, victim.bytes.clone())).unzip();
	if !victim_ids.is_empty() {
		diffs.push(Diff::remove(from_encoded_bytes(shape, &victim_ids, &victim_rows)?));
	}
	diffs.push(Diff::insert(from_encoded_bytes(shape, ids, rows)?));
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
		evicted: &[Option<Evicted>],
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
	evicted: &[Option<Evicted>],
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
		evicted.iter().flatten().map(|victim| (victim.row, victim.bytes.clone())).unzip();
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

	let victims: Vec<(TaggedKey, EncodedBytes)> = evicted
		.iter()
		.enumerate()
		.filter_map(|(idx, victim)| {
			victim.as_ref().map(|victim| {
				(
					ringbuffer_key(ringbuffer, partitions.get(idx).copied(), victim.row),
					victim.bytes.clone(),
				)
			})
		})
		.collect();
	let written = {
		let mut dml = DmlTransaction::over(txn.transaction());
		let mut writer = RingWriter::new(&mut dml);
		if !victims.is_empty() {
			writer.delete(victims)?;
		}
		writer.insert(keys.into_iter().zip(written).map(|(key, row)| (key, row, SetPre::Absent)).collect())?
			.written
	};

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

	let posts = RingWriter::new(&mut DmlTransaction::over(txn.transaction()))
		.update(keys.into_iter().zip(posts).collect())?
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

	let removed = RingWriter::new(&mut DmlTransaction::over(txn.transaction()))
		.delete(keys.into_iter().zip(removed).collect())?
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
		evicted: &[Option<Evicted>],
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
		evicted: &[Option<Evicted>],
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
		evicted: &[Option<Evicted>],
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
		let ids = [RowNumber(2), RowNumber(3)];
		let victim = Evicted {
			row: RowNumber(1),
			bytes: row(3),
		};
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		let written = txn
			.write_ringbuffer(&ringbuffer(), &shape, &[], &ids, &[row(5), row(6)], &[Some(victim), None])
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
			Delta::Remove { announce: RemoveAnnounce::Announced { pre }, .. } if pre == row(3)
		));
	}

	#[test]
	fn an_insert_without_evictions_fires_no_delete_hook() {
		// A delete hook for a fresh row would report a row that never existed.
		let mut txn = create_test_admin_transaction();
		let log = hooked(&mut txn);
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		txn.write_ringbuffer(&ringbuffer(), &shape, &[], &[RowNumber(1)], &[row(5)], &[None]).unwrap();
		assert_eq!(*log.lock().unwrap(), vec!["pre_insert".to_string(), "post_insert [5]".to_string()]);
	}

	#[test]
	fn an_insert_removes_the_evicted_row_with_its_pre_and_writes_fresh_rows_absent() {
		// A victim without its pre, or a new row with one, makes the lift misreport the eviction.
		let mut txn = create_test_admin_transaction();
		let ids = [RowNumber(2), RowNumber(3)];
		let victim = Evicted {
			row: RowNumber(1),
			bytes: row(3),
		};
		let shape = row_shape_from_columns(RowFamily::RingBuffer, &ringbuffer().columns);
		txn.write_ringbuffer(&ringbuffer(), &shape, &[], &ids, &[row(5), row(6)], &[Some(victim), None])
			.unwrap();
		let pres: Vec<SetPre> = ids
			.iter()
			.map(|&id| match pending(&txn, &ringbuffer_key(&ringbuffer(), None, id)) {
				Delta::Set {
					pre,
					..
				} => pre,
				other => panic!("expected a set, got {other:?}"),
			})
			.collect();
		assert_eq!(pres, vec![SetPre::Absent, SetPre::Absent], "a fresh row number never has a stored row");
		assert!(
			matches!(
				pending(&txn, &ringbuffer_key(&ringbuffer(), None, RowNumber(1))),
				Delta::Remove { announce: RemoveAnnounce::Announced { pre }, .. } if pre == row(3)
			),
			"the victim is removed under its own key with its bytes as pre"
		);
	}

	#[test]
	fn an_update_keeps_the_callers_pre_for_its_hook_and_tells_the_store_none() {
		// Ring update DML gives the store no pre today; passing one would change the commit log pre-image
		// source.
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
		// With nothing committed the displayed row is the pre-image; dropping it would lose the commit log
		// remove.
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
mod row_number_tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::id::{NamespaceId, RingBufferId},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;

	use super::*;
	use crate::vm::executor::Executor;

	#[test]
	fn released_row_numbers_are_never_handed_out_again_across_sessions() {
		// Row numbers are user visible, so a reused one changes what a query returns.
		let capacity = 4;
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
		let first = reserve_ring_rows(catalog, &mut txn, &ringbuffer, capacity as usize).unwrap();
		let fresh: Vec<RowNumber> = (first..first + capacity).map(RowNumber).collect();
		let placed = place_ring_rows(catalog, &mut txn, &ringbuffer, &mut ring, &[(None, fresh)]).unwrap().remove(0);
		for placement in &placed {
			let mut row = shape.allocate_ringbuffer();
			row.set_sequence(placement.row.0);
			txn.set(vec![(ringbuffer_key(&ringbuffer, None, placement.row), row.freeze_bytes())]).unwrap();
		}
		save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();
		let rows: Vec<RowNumber> = placed.iter().map(|placement| placement.row).collect();

		let mut ring = Ring::new(capacity);
		release_ring_rows(catalog, &mut txn, &ringbuffer, &mut ring, &[(None, rows[..2].to_vec())]).unwrap();
		save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();

		let mut last = rows[rows.len() - 1];
		for _ in 0..2 {
			let mut ring = Ring::new(capacity);
			let first = reserve_ring_rows(catalog, &mut txn, &ringbuffer, 1).unwrap();
			let next =
				place_ring_rows(catalog, &mut txn, &ringbuffer, &mut ring, &[(None, vec![RowNumber(first)])]).unwrap().remove(0);
			assert_eq!(next[0].row.0, last.0 + 1, "a released row number is never handed out again");
			assert_eq!(next[0].evicted, None);
			save_ring(catalog, &mut txn, &ringbuffer, &ring).unwrap();
			last = next[0].row;
		}
	}
}
