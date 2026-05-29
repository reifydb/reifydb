// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{ringbuffer::RingBuffer, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_transaction::{
	interceptor::ringbuffer_row::RingBufferRowInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::Result;

fn build_ringbuffer_insert_change(
	rb: &RingBuffer,
	shape: &RowShape,
	row_number: RowNumber,
	encoded: &EncodedRow,
) -> Change {
	let ids = [row_number];
	let rows = [encoded.clone()];
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::insert(Columns::from_encoded_rows(shape, &ids, &rows))],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_update_change(
	rb: &RingBuffer,
	row_number: RowNumber,
	pre: &EncodedRow,
	post: &EncodedRow,
) -> Change {
	let shape: RowShape = (&rb.columns).into();
	let ids = [row_number];
	let pres = [pre.clone()];
	let posts = [post.clone()];
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::update(
			Columns::from_encoded_rows(&shape, &ids, &pres),
			Columns::from_encoded_rows(&shape, &ids, &posts),
		)],
		changed_at: DateTime::default(),
	}
}

fn build_ringbuffer_remove_change(rb: &RingBuffer, row_number: RowNumber, encoded: &EncodedRow) -> Change {
	let shape: RowShape = (&rb.columns).into();
	let ids = [row_number];
	let rows = [encoded.clone()];
	Change {
		origin: ChangeOrigin::Shape(ShapeId::ringbuffer(rb.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(Columns::from_encoded_rows(&shape, &ids, &rows))],
		changed_at: DateTime::default(),
	}
}

pub trait RingBufferOperations {
	fn insert_ringbuffer(&mut self, ringbuffer: RingBuffer, row: EncodedRow) -> Result<RowNumber>;

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow>;

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow>;

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow>;
}

impl RingBufferOperations for CommandTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		let pre = self.get(&key)?.map(|v| v.row);

		if let Some(ref existing) = pre {
			let ids = [row_number];
			let existing_rows = [existing.clone()];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
		}

		let mut rows_buf = [row];
		RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
		let [row] = rows_buf;

		self.set(&key, row.clone())?;

		let ids = [row_number];
		let rows = [row.clone()];
		RingBufferRowInterceptor::post_insert(self, ringbuffer, &ids, &rows)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &row));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let pre = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let mut rows_buf = [row];
		let ids = [id];
		RingBufferRowInterceptor::pre_update(self, &ringbuffer, &ids, &mut rows_buf)?;
		let [row] = rows_buf;

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, row.clone())?;

		let posts = [row.clone()];
		let pres = [pre.clone()];
		RingBufferRowInterceptor::post_update(self, &ringbuffer, &ids, &posts, &pres)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &row));

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.row);

		let ids = [id];
		RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.unset(&key, pre_for_cdc.clone())?;

		let pre_rows = [pre_for_cdc.clone()];
		RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &pre_rows)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for AdminTransaction {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, row_number);

		let pre = self.get(&key)?.map(|v| v.row);

		if let Some(ref existing) = pre {
			let ids = [row_number];
			let existing_rows = [existing.clone()];
			RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;
			RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &existing_rows)?;
		}

		let mut rows_buf = [row];
		RingBufferRowInterceptor::pre_insert(self, ringbuffer, &mut rows_buf)?;
		let [row] = rows_buf;

		self.set(&key, row.clone())?;

		let ids = [row_number];
		let rows = [row.clone()];
		RingBufferRowInterceptor::post_insert(self, ringbuffer, &ids, &rows)?;

		if let Some(pre_row) = pre.as_ref() {
			self.track_flow_change(build_ringbuffer_update_change(ringbuffer, row_number, pre_row, &row));
		} else {
			self.track_flow_change(build_ringbuffer_insert_change(ringbuffer, shape, row_number, &row));
		}

		Ok(row)
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let pre = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(row),
		};

		let mut rows_buf = [row];
		let ids = [id];
		RingBufferRowInterceptor::pre_update(self, &ringbuffer, &ids, &mut rows_buf)?;
		let [row] = rows_buf;

		if self.get_committed(&key)?.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.set(&key, row.clone())?;

		let posts = [row.clone()];
		let pres = [pre.clone()];
		RingBufferRowInterceptor::post_update(self, &ringbuffer, &ids, &posts, &pres)?;

		self.track_flow_change(build_ringbuffer_update_change(&ringbuffer, id, &pre, &row));

		Ok(row)
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		let key = RowKey::encoded(ringbuffer.id, id);

		let displayed = match self.get(&key)? {
			Some(v) => v.row,
			None => return Ok(EncodedRow(CowVec::new(vec![]))),
		};
		let committed = self.get_committed(&key)?.map(|v| v.row);

		let ids = [id];
		RingBufferRowInterceptor::pre_delete(self, ringbuffer, &ids)?;

		let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());

		if committed.is_some() {
			self.mark_preexisting(&key)?;
		}
		self.unset(&key, pre_for_cdc.clone())?;

		let pre_rows = [pre_for_cdc.clone()];
		RingBufferRowInterceptor::post_delete(self, ringbuffer, &ids, &pre_rows)?;

		self.track_flow_change(build_ringbuffer_remove_change(ringbuffer, id, &pre_for_cdc));

		Ok(displayed)
	}
}

impl RingBufferOperations for Transaction<'_> {
	fn insert_ringbuffer(&mut self, _ringbuffer: RingBuffer, _row: EncodedRow) -> Result<RowNumber> {
		unimplemented!(
			"Ring buffer insert must be called with explicit row_number through insert_ringbuffer_at"
		)
	}

	fn insert_ringbuffer_at(
		&mut self,
		ringbuffer: &RingBuffer,
		shape: &RowShape,
		row_number: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Admin(txn) => txn.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Test(t) => t.inner.insert_ringbuffer_at(ringbuffer, shape, row_number, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn update_ringbuffer(&mut self, ringbuffer: RingBuffer, id: RowNumber, row: EncodedRow) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Admin(txn) => txn.update_ringbuffer(ringbuffer, id, row),
			Transaction::Test(t) => t.inner.update_ringbuffer(ringbuffer, id, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn remove_from_ringbuffer(&mut self, ringbuffer: &RingBuffer, id: RowNumber) -> Result<EncodedRow> {
		match self {
			Transaction::Command(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Admin(txn) => txn.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Test(t) => t.inner.remove_from_ringbuffer(ringbuffer, id),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}
}
