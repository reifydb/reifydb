// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{shape::ShapeId, table::Table},
		change::{Change, ChangeOrigin, Diff},
	},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_transaction::{
	change::{RowChange, TableRowInsertion},
	interceptor::table_row::TableRowInterceptor,
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use smallvec::smallvec;

use crate::Result;

fn build_table_insert_change(table: &Table, shape: &RowShape, ids: &[RowNumber], rows: &[EncodedRow]) -> Change {
	Change {
		origin: ChangeOrigin::Shape(ShapeId::Table(table.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::insert(Columns::from_encoded_rows(shape, ids, rows))],
		changed_at: DateTime::default(),
	}
}

fn build_table_update_change(
	table: &Table,
	shape: &RowShape,
	ids: &[RowNumber],
	pres: &[EncodedRow],
	posts: &[EncodedRow],
) -> Change {
	Change {
		origin: ChangeOrigin::Shape(ShapeId::Table(table.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::update(
			Columns::from_encoded_rows(shape, ids, pres),
			Columns::from_encoded_rows(shape, ids, posts),
		)],
		changed_at: DateTime::default(),
	}
}

fn build_table_remove_change(table: &Table, shape: &RowShape, ids: &[RowNumber], rows: &[EncodedRow]) -> Change {
	Change {
		origin: ChangeOrigin::Shape(ShapeId::Table(table.id)),
		version: CommitVersion(0),
		diffs: smallvec![Diff::remove(Columns::from_encoded_rows(shape, ids, rows))],
		changed_at: DateTime::default(),
	}
}

pub(crate) trait TableOperations {
	fn insert_table(
		&mut self,
		table: &Table,
		shape: &RowShape,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()>;

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<Vec<(RowNumber, EncodedRow)>>;

	fn remove_from_table(&mut self, table: &Table, ids: &[RowNumber]) -> Result<Vec<(RowNumber, EncodedRow)>>;
}

impl TableOperations for CommandTransaction {
	fn insert_table(
		&mut self,
		table: &Table,
		shape: &RowShape,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(());
		}

		TableRowInterceptor::pre_insert(self, table, ids, rows)?;

		for (row, &row_number) in rows.iter().zip(ids.iter()) {
			self.set(&RowKey::encoded(table.id, row_number), row.clone())?;
		}

		TableRowInterceptor::post_insert(self, table, ids, rows)?;

		let row_changes: Vec<RowChange> = ids
			.iter()
			.zip(rows.iter())
			.map(|(&row_number, row)| {
				RowChange::TableInsert(TableRowInsertion {
					table_id: table.id,
					row_number,
					encoded: row.clone(),
				})
			})
			.collect();
		self.track_row_change(&row_changes);

		self.track_flow_change(build_table_insert_change(table, shape, ids, rows));

		Ok(())
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_update(self, table, ids, rows)?;

		let mut matched_indices: Vec<usize> = Vec::with_capacity(ids.len());
		let mut pres: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let key = RowKey::encoded(table.id, row_number);
			let pre = match self.get(&key)? {
				Some(v) => v.row,
				None => continue,
			};
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, rows[idx].clone())?;
			matched_indices.push(idx);
			pres.push(pre);
		}

		if matched_indices.is_empty() {
			return Ok(Vec::new());
		}

		let matched_ids: Vec<RowNumber> = matched_indices.iter().map(|&i| ids[i]).collect();
		let matched_posts: Vec<EncodedRow> = matched_indices.iter().map(|&i| rows[i].clone()).collect();

		TableRowInterceptor::post_update(self, table, &matched_ids, &matched_posts, &pres)?;

		let shape: RowShape = (&table.columns).into();
		self.track_flow_change(build_table_update_change(table, &shape, &matched_ids, &pres, &matched_posts));

		Ok(matched_ids.into_iter().zip(matched_posts).collect())
	}

	fn remove_from_table(&mut self, table: &Table, ids: &[RowNumber]) -> Result<Vec<(RowNumber, EncodedRow)>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut matched_ids: Vec<RowNumber> = Vec::with_capacity(ids.len());
		let mut displayed_rows: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		let mut pre_for_cdc_rows: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		for &row_number in ids.iter() {
			let key = RowKey::encoded(table.id, row_number);
			let displayed = match self.get(&key)? {
				Some(v) => v.row,
				None => continue,
			};
			let committed = self.get_committed(&key)?.map(|v| v.row);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());
			matched_ids.push(row_number);
			displayed_rows.push(displayed);
			pre_for_cdc_rows.push(pre_for_cdc);
		}

		if matched_ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_delete(self, table, &matched_ids)?;

		for (i, &row_number) in matched_ids.iter().enumerate() {
			let key = RowKey::encoded(table.id, row_number);
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.unset(&key, pre_for_cdc_rows[i].clone())?;
		}

		TableRowInterceptor::post_delete(self, table, &matched_ids, &pre_for_cdc_rows)?;

		let shape: RowShape = (&table.columns).into();
		self.track_flow_change(build_table_remove_change(table, &shape, &matched_ids, &pre_for_cdc_rows));

		Ok(matched_ids.into_iter().zip(displayed_rows).collect())
	}
}

impl TableOperations for AdminTransaction {
	fn insert_table(
		&mut self,
		table: &Table,
		shape: &RowShape,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(());
		}

		TableRowInterceptor::pre_insert(self, table, ids, rows)?;

		for (row, &row_number) in rows.iter().zip(ids.iter()) {
			self.set(&RowKey::encoded(table.id, row_number), row.clone())?;
		}

		TableRowInterceptor::post_insert(self, table, ids, rows)?;

		let row_changes: Vec<RowChange> = ids
			.iter()
			.zip(rows.iter())
			.map(|(&row_number, row)| {
				RowChange::TableInsert(TableRowInsertion {
					table_id: table.id,
					row_number,
					encoded: row.clone(),
				})
			})
			.collect();
		self.track_row_change(&row_changes);

		self.track_flow_change(build_table_insert_change(table, shape, ids, rows));

		Ok(())
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_update(self, table, ids, rows)?;

		let mut matched_indices: Vec<usize> = Vec::with_capacity(ids.len());
		let mut pres: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let key = RowKey::encoded(table.id, row_number);
			let pre = match self.get(&key)? {
				Some(v) => v.row,
				None => continue,
			};
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, rows[idx].clone())?;
			matched_indices.push(idx);
			pres.push(pre);
		}

		if matched_indices.is_empty() {
			return Ok(Vec::new());
		}

		let matched_ids: Vec<RowNumber> = matched_indices.iter().map(|&i| ids[i]).collect();
		let matched_posts: Vec<EncodedRow> = matched_indices.iter().map(|&i| rows[i].clone()).collect();

		TableRowInterceptor::post_update(self, table, &matched_ids, &matched_posts, &pres)?;

		let shape: RowShape = (&table.columns).into();
		self.track_flow_change(build_table_update_change(table, &shape, &matched_ids, &pres, &matched_posts));

		Ok(matched_ids.into_iter().zip(matched_posts).collect())
	}

	fn remove_from_table(&mut self, table: &Table, ids: &[RowNumber]) -> Result<Vec<(RowNumber, EncodedRow)>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut matched_ids: Vec<RowNumber> = Vec::with_capacity(ids.len());
		let mut displayed_rows: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		let mut pre_for_cdc_rows: Vec<EncodedRow> = Vec::with_capacity(ids.len());
		for &row_number in ids.iter() {
			let key = RowKey::encoded(table.id, row_number);
			let displayed = match self.get(&key)? {
				Some(v) => v.row,
				None => continue,
			};
			let committed = self.get_committed(&key)?.map(|v| v.row);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());
			matched_ids.push(row_number);
			displayed_rows.push(displayed);
			pre_for_cdc_rows.push(pre_for_cdc);
		}

		if matched_ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_delete(self, table, &matched_ids)?;

		for (i, &row_number) in matched_ids.iter().enumerate() {
			let key = RowKey::encoded(table.id, row_number);
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.unset(&key, pre_for_cdc_rows[i].clone())?;
		}

		TableRowInterceptor::post_delete(self, table, &matched_ids, &pre_for_cdc_rows)?;

		let shape: RowShape = (&table.columns).into();
		self.track_flow_change(build_table_remove_change(table, &shape, &matched_ids, &pre_for_cdc_rows));

		Ok(matched_ids.into_iter().zip(displayed_rows).collect())
	}
}

impl TableOperations for Transaction<'_> {
	fn insert_table(
		&mut self,
		table: &Table,
		shape: &RowShape,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_table(table, shape, ids, rows),
			Transaction::Admin(txn) => txn.insert_table(table, shape, ids, rows),
			Transaction::Test(t) => t.inner.insert_table(table, shape, ids, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		match self {
			Transaction::Command(txn) => txn.update_table(table, ids, rows),
			Transaction::Admin(txn) => txn.update_table(table, ids, rows),
			Transaction::Test(t) => t.inner.update_table(table, ids, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}

	fn remove_from_table(&mut self, table: &Table, ids: &[RowNumber]) -> Result<Vec<(RowNumber, EncodedRow)>> {
		match self {
			Transaction::Command(txn) => txn.remove_from_table(table, ids),
			Transaction::Admin(txn) => txn.remove_from_table(table, ids),
			Transaction::Test(t) => t.inner.remove_from_table(table, ids),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Replica(_) => panic!("Write operations not supported on Replica transaction"),
		}
	}
}
