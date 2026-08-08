// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, shape::RowShape},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			object::ObjectId,
			ringbuffer::{RingBuffer, RingBufferMetadata},
		},
		change::{Change, ChangeOrigin, Diff},
	},
	key::{
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
	row::row_shape_from_columns,
	value::column::columns::Columns,
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

use crate::{
	Result,
	error::EngineError,
	partition::{partition_col_indices, partition_values},
};

fn ringbuffer_key(ringbuffer: &RingBuffer, partition: Option<Partition>, row_number: RowNumber) -> EncodedKey {
	match partition {
		None => RowKey::encoded(ringbuffer.id, row_number),
		Some(partition) => PartitionedRowKey::encoded(
			ObjectId::ringbuffer(ringbuffer.id),
			partition,
			RowLocator::Row(row_number),
		),
	}
}

fn build_ringbuffer_insert_change(
	rb: &RingBuffer,
	shape: &RowShape,
	row_number: RowNumber,
	encoded: &EncodedBytes,
) -> Change {
	let ids = [row_number];
	let rows = [encoded.clone()];
	Change {
		origin: ChangeOrigin::Object(ObjectId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::insert(Columns::from_encoded_bytes(shape, &ids, &rows))],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_update_change(
	rb: &RingBuffer,
	row_number: RowNumber,
	pre: &EncodedBytes,
	post: &EncodedBytes,
) -> Change {
	let shape = row_shape_from_columns(&rb.columns);
	let ids = [row_number];
	let pres = [pre.clone()];
	let posts = [post.clone()];
	Change {
		origin: ChangeOrigin::Object(ObjectId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::update(
			Columns::from_encoded_bytes(&shape, &ids, &pres),
			Columns::from_encoded_bytes(&shape, &ids, &posts),
		)],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_remove_change(rb: &RingBuffer, row_number: RowNumber, encoded: &EncodedBytes) -> Change {
	let shape = row_shape_from_columns(&rb.columns);
	let ids = [row_number];
	let rows = [encoded.clone()];
	Change {
		origin: ChangeOrigin::Object(ObjectId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(Columns::from_encoded_bytes(&shape, &ids, &rows))],
		changed_at: DateTime::default(),
	}
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
	fn insert_ringbuffer(&mut self, ringbuffer: RingBuffer, bytes: EncodedBytes) -> Result<RowNumber>;

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partition: Option<Partition>,
		row_number: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes>;

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes>;

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
	) -> Result<EncodedBytes>;
}

impl RingBufferOperations for CommandTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedBytes) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partition: Option<Partition>,
		row_number: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(ringbuffer, partition, row_number);

		let pre = self.get(&key)?.map(|v| v.bytes);

		if let Some(ref existing) = pre {
			let ids = [row_number];
			let existing_rows = [existing.clone()];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
		}

		let mut rows_buf = [bytes.thaw()];
		RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
		let [bytes] = rows_buf;
		let bytes = bytes.freeze();

		self.set(&key, bytes.clone())?;

		let ids = [row_number];
		let rows = [bytes.clone()];
		RingBufferRowInterceptor::post_insert(self, ringbuffer, &ids, &rows)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &bytes));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &bytes));
		}

		Ok(bytes)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(&ringbuffer, partition, id);

		let pre = match self.get(&key)? {
			Some(v) => v.bytes,
			None => return Ok(bytes),
		};

		let mut rows_buf = [bytes.thaw()];
		let ids = [id];
		RingBufferRowInterceptor::pre_update(self, &ringbuffer, &ids, &mut rows_buf)?;
		let [bytes] = rows_buf;
		let bytes = bytes.freeze();

		if let Some(expected) = partition {
			let shape = row_shape_from_columns(&ringbuffer.columns);
			let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
			if Partition::of(&partition_values(&shape, &bytes, &indices)) != expected {
				return Err(EngineError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}
		}

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, bytes.clone())?;

		let posts = [bytes.clone()];
		let pres = [pre.clone()];
		RingBufferRowInterceptor::post_update(self, &ringbuffer, &ids, &posts, &pres)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &bytes));

		Ok(bytes)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(ringbuffer, partition, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.bytes,
			None => return Ok(EncodedBytes(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.bytes);

		let ids = [id];
		RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.remove_with_pre(&key, pre_for_cdc.clone())?;

		let pre_rows = [pre_for_cdc.clone()];
		RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &pre_rows)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for AdminTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedBytes) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partition: Option<Partition>,
		row_number: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(ringbuffer, partition, row_number);

		let pre = self.get(&key)?.map(|v| v.bytes);

		if let Some(ref existing) = pre {
			let ids = [row_number];
			let existing_rows = [existing.clone()];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
		}

		let mut rows_buf = [bytes.thaw()];
		RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
		let [bytes] = rows_buf;
		let bytes = bytes.freeze();

		self.set(&key, bytes.clone())?;

		let ids = [row_number];
		let rows = [bytes.clone()];
		RingBufferRowInterceptor::post_insert(self, ringbuffer, &ids, &rows)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &bytes));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &bytes));
		}

		Ok(bytes)
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(&ringbuffer, partition, id);

		let pre = match self.get(&key)? {
			Some(v) => v.bytes,
			None => return Ok(bytes),
		};

		let mut rows_buf = [bytes.thaw()];
		let ids = [id];
		RingBufferRowInterceptor::pre_update(self, &ringbuffer, &ids, &mut rows_buf)?;
		let [bytes] = rows_buf;
		let bytes = bytes.freeze();

		if let Some(expected) = partition {
			let shape = row_shape_from_columns(&ringbuffer.columns);
			let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
			if Partition::of(&partition_values(&shape, &bytes, &indices)) != expected {
				return Err(EngineError::ImmutablePartitionColumn {
					object: ObjectId::ringbuffer(ringbuffer.id),
				}
				.into());
			}
		}

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, bytes.clone())?;

		let posts = [bytes.clone()];
		let pres = [pre.clone()];
		RingBufferRowInterceptor::post_update(self, &ringbuffer, &ids, &posts, &pres)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &bytes));

		Ok(bytes)
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
	) -> Result<EncodedBytes> {
		let key = ringbuffer_key(ringbuffer, partition, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.bytes,
			None => return Ok(EncodedBytes(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.bytes);

		let ids = [id];
		RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.remove_with_pre(&key, pre_for_cdc.clone())?;

		let pre_rows = [pre_for_cdc.clone()];
		RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &pre_rows)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedBytes) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		partition: Option<Partition>,
		row_number: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		match self {
			Transaction::Command(txn) => {
				txn.insert_ringbuffer_at(ringbuffer, shape, partition, row_number, bytes)
			}
			Transaction::Admin(txn) => {
				txn.insert_ringbuffer_at(ringbuffer, shape, partition, row_number, bytes)
			}
			Transaction::Test(t) => {
				t.inner.insert_ringbuffer_at(ringbuffer, shape, partition, row_number, bytes)
			}
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn update_ringbuffer(
		&mut self,
		ringbuffer: RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
		bytes: EncodedBytes,
	) -> Result<EncodedBytes> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, partition, id, bytes),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, partition, id, bytes),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, partition, id, bytes),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn remove_from_ringbuffer(
		&mut self,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
		id: RowNumber,
	) -> Result<EncodedBytes> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, partition, id),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, partition, id),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, partition, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}
}
