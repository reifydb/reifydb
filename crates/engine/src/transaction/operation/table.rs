// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::{EncodedBytes, RowBuilder},
	shape::{RowFamily, RowShape},
	table::EncodedTableRowBuilder,
};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	error::diagnostic::index::primary_key_changed_by_hook,
	interface::{
		catalog::{object::ObjectId, table::Table},
		change::{Change, ChangeOrigin, Diff},
	},
	key::any::TaggedKey,
	partition::PartitionError,
	row::row_shape_from_columns,
	value::batch::from_encoded_bytes,
};
use reifydb_transaction::{
	change::{RowChange, TableRowInsertion},
	interceptor::{WithInterceptors, table_row::TableRowInterceptor},
	transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction, write::Write},
};
use reifydb_value::{
	fragment::Fragment,
	return_error,
	value::{Value, datetime::DateTime, partition::Partition, row_number::RowNumber},
};
use smallvec::smallvec;

use crate::{
	Result,
	partition::{row_key_from_partition, table_partition_of_row},
};

fn build_table_insert_change(
	table: &Table,
	shape: &RowShape,
	ids: &[RowNumber],
	bytes_slice: &[EncodedBytes],
) -> Result<Change> {
	Ok(Change {
		origin: ChangeOrigin::Object(ObjectId::Table(table.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::insert(from_encoded_bytes(shape, ids, bytes_slice)?)],
		changed_at: DateTime::default(),
	})
}

fn build_table_update_change(
	table: &Table,
	shape: &RowShape,
	ids: &[RowNumber],
	pres: &[EncodedBytes],
	posts: &[EncodedBytes],
) -> Result<Change> {
	Ok(Change {
		origin: ChangeOrigin::Object(ObjectId::Table(table.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::update(
			from_encoded_bytes(shape, ids, pres)?,
			from_encoded_bytes(shape, ids, posts)?,
		)],
		changed_at: DateTime::default(),
	})
}

fn build_table_remove_change(
	table: &Table,
	shape: &RowShape,
	ids: &[RowNumber],
	bytes_slice: &[EncodedBytes],
) -> Result<Change> {
	Ok(Change {
		origin: ChangeOrigin::Object(ObjectId::Table(table.id)),
		version: ChangeVersion::from(CommitVersion(0)),
		diffs: smallvec![Diff::remove(from_encoded_bytes(shape, ids, bytes_slice)?)],
		changed_at: DateTime::default(),
	})
}

pub struct TableInsert<'a> {
	pub ids: &'a [RowNumber],
	pub partitions: &'a [Partition],
	pub rows: &'a mut [EncodedTableRowBuilder],
	pub index: Vec<(TaggedKey, EncodedBytes)>,
	pub key_columns: &'a [usize],
}

fn insert_rows<T: WithInterceptors + Write>(
	txn: &mut T,
	table: &Table,
	shape: &RowShape,
	insert: TableInsert<'_>,
) -> Result<()> {
	let TableInsert {
		ids,
		partitions,
		rows,
		index,
		key_columns,
	} = insert;
	assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
	assert!(
		if table.partition_by.is_empty() {
			partitions.is_empty()
		} else {
			partitions.len() == ids.len()
		},
		"a partitioned table needs one partition per row, an unpartitioned table none"
	);
	if ids.is_empty() {
		return Ok(());
	}

	if !txn.table_row_pre_insert_interceptors().is_empty() {
		let keys = key_cells(shape, rows, key_columns);
		TableRowInterceptor::pre_insert(txn, table, ids, rows)?;
		if rows.iter()
			.zip(partitions)
			.any(|(row, &partition)| table_partition_of_row(table, shape, row) != partition)
		{
			return Err(PartitionError::PartitionColumnChangedByHook {
				object: ObjectId::Table(table.id),
			}
			.into());
		}
		if key_cells(shape, rows, key_columns) != keys {
			let key_columns = key_columns.iter().map(|&index| table.columns[index].name.clone()).collect();
			return_error!(primary_key_changed_by_hook(Fragment::None, table.name.clone(), key_columns));
		}
	}

	let frozen: Vec<EncodedBytes> = rows.iter().map(|row| row.clone().freeze_bytes()).collect();

	let mut entries: Vec<(TaggedKey, EncodedBytes)> = Vec::with_capacity(frozen.len() + index.len());
	for (row_index, (row, &row_number)) in frozen.iter().zip(ids).enumerate() {
		entries.push((
			row_key_from_partition(table.id, partitions.get(row_index).copied(), row_number),
			row.clone(),
		));
	}
	entries.extend(index);
	txn.set_many(entries)?;

	TableRowInterceptor::post_insert(txn, table, ids, &frozen)?;

	let row_changes: Vec<RowChange> = ids
		.iter()
		.zip(frozen.iter())
		.map(|(&row_number, row)| {
			RowChange::TableInsert(TableRowInsertion {
				table_id: table.id,
				row_number,
				encoded: row.clone(),
			})
		})
		.collect();
	txn.track_row_change(&row_changes);

	txn.track_flow_change(build_table_insert_change(table, shape, ids, &frozen)?);

	Ok(())
}

fn key_cells(shape: &RowShape, rows: &[EncodedTableRowBuilder], key_columns: &[usize]) -> Vec<Value> {
	rows.iter().flat_map(|row| key_columns.iter().map(|&index| shape.get_value(row, index))).collect()
}

pub trait TableOperations {
	fn insert_table(&mut self, table: &Table, shape: &RowShape, insert: TableInsert<'_>) -> Result<()>;

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
		rows: &mut [EncodedTableRowBuilder],
	) -> Result<Vec<(RowNumber, EncodedBytes)>>;

	fn remove_from_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
	) -> Result<Vec<(RowNumber, EncodedBytes)>>;
}

impl TableOperations for CommandTransaction {
	fn insert_table(&mut self, table: &Table, shape: &RowShape, insert: TableInsert<'_>) -> Result<()> {
		insert_rows(self, table, shape, insert)
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
		rows: &mut [EncodedTableRowBuilder],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let shape = row_shape_from_columns(RowFamily::Table, &table.columns);

		TableRowInterceptor::pre_update(self, table, ids, rows)?;

		if !table.partition_by.is_empty() {
			for (idx, row) in rows.iter().enumerate() {
				if let Some(&expected) = partitions.get(idx)
					&& table_partition_of_row(table, &shape, row) != expected
				{
					return Err(PartitionError::ImmutablePartitionColumn {
						object: ObjectId::Table(table.id),
					}
					.into());
				}
			}
		}

		let mut matched_indices: Vec<usize> = Vec::with_capacity(ids.len());
		let mut pres: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let key = row_key_from_partition(table.id, partitions.get(idx).copied(), row_number);
			let pre = match self.get(&key)? {
				Some(v) => v.bytes,
				None => continue,
			};
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, rows[idx].clone().freeze())?;
			matched_indices.push(idx);
			pres.push(pre);
		}

		if matched_indices.is_empty() {
			return Ok(Vec::new());
		}

		let matched_ids: Vec<RowNumber> = matched_indices.iter().map(|&i| ids[i]).collect();
		let matched_posts: Vec<EncodedBytes> =
			matched_indices.iter().map(|&i| rows[i].clone().freeze_bytes()).collect();

		TableRowInterceptor::post_update(self, table, &matched_ids, &matched_posts, &pres)?;

		self.track_flow_change(build_table_update_change(table, &shape, &matched_ids, &pres, &matched_posts)?);

		Ok(matched_ids.into_iter().zip(matched_posts).collect())
	}

	fn remove_from_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut matched_ids: Vec<RowNumber> = Vec::with_capacity(ids.len());
		let mut matched_partitions: Vec<Option<Partition>> = Vec::with_capacity(ids.len());
		let mut displayed_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		let mut pre_for_cdc_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let partition = partitions.get(idx).copied();
			let key = row_key_from_partition(table.id, partition, row_number);
			let displayed = match self.get(&key)? {
				Some(v) => v.bytes,
				None => continue,
			};
			let committed = self.get_committed(&key)?.map(|v| v.bytes);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());
			matched_ids.push(row_number);
			matched_partitions.push(partition);
			displayed_bytes_vec.push(displayed);
			pre_for_cdc_bytes_vec.push(pre_for_cdc);
		}

		if matched_ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_delete(self, table, &matched_ids)?;

		for (i, &row_number) in matched_ids.iter().enumerate() {
			let key = row_key_from_partition(table.id, matched_partitions[i], row_number);
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.remove_with_pre(&key, pre_for_cdc_bytes_vec[i].clone())?;
		}

		TableRowInterceptor::post_delete(self, table, &matched_ids, &pre_for_cdc_bytes_vec)?;

		let shape = row_shape_from_columns(RowFamily::Table, &table.columns);
		self.track_flow_change(build_table_remove_change(table, &shape, &matched_ids, &displayed_bytes_vec)?);

		Ok(matched_ids.into_iter().zip(displayed_bytes_vec).collect())
	}
}

impl TableOperations for AdminTransaction {
	fn insert_table(&mut self, table: &Table, shape: &RowShape, insert: TableInsert<'_>) -> Result<()> {
		insert_rows(self, table, shape, insert)
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
		rows: &mut [EncodedTableRowBuilder],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let shape = row_shape_from_columns(RowFamily::Table, &table.columns);

		TableRowInterceptor::pre_update(self, table, ids, rows)?;

		if !table.partition_by.is_empty() {
			for (idx, row) in rows.iter().enumerate() {
				if let Some(&expected) = partitions.get(idx)
					&& table_partition_of_row(table, &shape, row) != expected
				{
					return Err(PartitionError::ImmutablePartitionColumn {
						object: ObjectId::Table(table.id),
					}
					.into());
				}
			}
		}

		let mut matched_indices: Vec<usize> = Vec::with_capacity(ids.len());
		let mut pres: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let key = row_key_from_partition(table.id, partitions.get(idx).copied(), row_number);
			let pre = match self.get(&key)? {
				Some(v) => v.bytes,
				None => continue,
			};
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.set(&key, rows[idx].clone().freeze())?;
			matched_indices.push(idx);
			pres.push(pre);
		}

		if matched_indices.is_empty() {
			return Ok(Vec::new());
		}

		let matched_ids: Vec<RowNumber> = matched_indices.iter().map(|&i| ids[i]).collect();
		let matched_posts: Vec<EncodedBytes> =
			matched_indices.iter().map(|&i| rows[i].clone().freeze_bytes()).collect();

		TableRowInterceptor::post_update(self, table, &matched_ids, &matched_posts, &pres)?;

		self.track_flow_change(build_table_update_change(table, &shape, &matched_ids, &pres, &matched_posts)?);

		Ok(matched_ids.into_iter().zip(matched_posts).collect())
	}

	fn remove_from_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		let mut matched_ids: Vec<RowNumber> = Vec::with_capacity(ids.len());
		let mut matched_partitions: Vec<Option<Partition>> = Vec::with_capacity(ids.len());
		let mut displayed_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		let mut pre_for_cdc_bytes_vec: Vec<EncodedBytes> = Vec::with_capacity(ids.len());
		for (idx, &row_number) in ids.iter().enumerate() {
			let partition = partitions.get(idx).copied();
			let key = row_key_from_partition(table.id, partition, row_number);
			let displayed = match self.get(&key)? {
				Some(v) => v.bytes,
				None => continue,
			};
			let committed = self.get_committed(&key)?.map(|v| v.bytes);
			let pre_for_cdc = committed.clone().unwrap_or_else(|| displayed.clone());
			matched_ids.push(row_number);
			matched_partitions.push(partition);
			displayed_bytes_vec.push(displayed);
			pre_for_cdc_bytes_vec.push(pre_for_cdc);
		}

		if matched_ids.is_empty() {
			return Ok(Vec::new());
		}

		TableRowInterceptor::pre_delete(self, table, &matched_ids)?;

		for (i, &row_number) in matched_ids.iter().enumerate() {
			let key = row_key_from_partition(table.id, matched_partitions[i], row_number);
			if self.get_committed(&key)?.is_some() {
				self.mark_preexisting(&key)?;
			}
			self.remove_with_pre(&key, pre_for_cdc_bytes_vec[i].clone())?;
		}

		TableRowInterceptor::post_delete(self, table, &matched_ids, &pre_for_cdc_bytes_vec)?;

		let shape = row_shape_from_columns(RowFamily::Table, &table.columns);
		self.track_flow_change(build_table_remove_change(table, &shape, &matched_ids, &displayed_bytes_vec)?);

		Ok(matched_ids.into_iter().zip(displayed_bytes_vec).collect())
	}
}

impl TableOperations for Transaction<'_> {
	fn insert_table(&mut self, table: &Table, shape: &RowShape, insert: TableInsert<'_>) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.insert_table(table, shape, insert),
			Transaction::Admin(txn) => txn.insert_table(table, shape, insert),
			Transaction::Test(t) => t.inner.insert_table(table, shape, insert),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn update_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
		rows: &mut [EncodedTableRowBuilder],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		match self {
			Transaction::Command(txn) => txn.update_table(table, ids, partitions, rows),
			Transaction::Admin(txn) => txn.update_table(table, ids, partitions, rows),
			Transaction::Test(t) => t.inner.update_table(table, ids, partitions, rows),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}

	fn remove_from_table(
		&mut self,
		table: &Table,
		ids: &[RowNumber],
		partitions: &[Partition],
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		match self {
			Transaction::Command(txn) => txn.remove_from_table(table, ids, partitions),
			Transaction::Admin(txn) => txn.remove_from_table(table, ids, partitions),
			Transaction::Test(t) => t.inner.remove_from_table(table, ids, partitions),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
		}
	}
}
