// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use reifydb_catalog::{
	catalog::Catalog,
	completeness::{Advanced, CompletenessWriter},
};
use reifydb_codec::row::{pod::EncodedPodRow, shape::RowShape, table::EncodedTableRow};
use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, IndexId, TableId},
		object::ObjectId,
		table::Table,
	},
	internal_error,
	key::{any::TaggedKey, catalog::IndexEntryKey},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, datetime::DateTime, row_number::RowNumber};

use crate::{
	Result,
	partition::row_key_from_partition,
	transaction::operation::table::{TableInsert, TableOperations, load_stored_shape},
	vm::instruction::dml::{
		primary_key::{PrimaryKeyEncoder, get_primary_key, primary_key_index},
		shape::get_or_create_table_shape,
	},
};

pub struct CompletenessWriterImpl {
	catalog: Catalog,
	clock: Clock,
}

impl CompletenessWriterImpl {
	pub fn new(catalog: Catalog, clock: Clock) -> Self {
		Self {
			catalog,
			clock,
		}
	}
}

impl CompletenessWriter for CompletenessWriterImpl {
	fn advance(&self, tx: &mut Transaction<'_>, object: ObjectId, at: DateTime) -> Result<Advanced> {
		let table = self.catalog.get_table(tx, TableId::SOURCE_COMPLETENESS)?;
		let Some(pk) = get_primary_key(&self.catalog, tx, &table)? else {
			return Err(internal_error!("{} has no primary key", table.name));
		};
		let shape = get_or_create_table_shape(&self.catalog, &table, tx)?;
		let encoder = PrimaryKeyEncoder::new(&pk, &table)?;
		let key_columns = encoder.key_columns();
		let through = column_index(&table, ColumnId::SOURCE_COMPLETENESS_COMPLETE_THROUGH)?;

		let mut row = shape.allocate_table();
		shape.set_value(
			&mut row,
			column_index(&table, ColumnId::SOURCE_COMPLETENESS_OBJECT_ID)?,
			&Value::Uint8(object.to_u64()),
		);
		shape.set_value(&mut row, through, &Value::DateTime(at));

		let index_key = encoder.encode(&shape, &row);
		let entry: TaggedKey = IndexEntryKey::new(table.id, IndexId::primary(pk.id), index_key.clone()).into();
		let now = self.clock.now();

		let Some(indexed) = tx.get(from_ref(&entry))?.pop().flatten() else {
			let ids = self.catalog.next_row_number_batch(tx, table.id, 1)?;
			row.set_timestamps(now, now);
			tx.insert_table(
				&table,
				&shape,
				TableInsert {
					ids: &ids,
					partitions: &[],
					rows: &mut [row],
					index: primary_key_index(&table, &pk, vec![index_key], &ids),
					key_columns: &key_columns,
				},
			)?;
			return Ok(Advanced::Inserted);
		};

		let row_number = indexed_row_number(&table, EncodedPodRow::view(&indexed.bytes).body())?;
		let row_key = row_key_from_partition(table.id, None, row_number);
		let Some(pre) = tx.get(from_ref(&row_key))?.pop().flatten() else {
			return Err(internal_error!("{} indexes row {:?} that does not exist", table.name, row_number));
		};
		let pre = pre.bytes;
		let pre_view = EncodedTableRow::view(&pre);
		let loaded: RowShape;
		let pre_shape = if pre_view.fingerprint() == shape.fingerprint() {
			&shape
		} else {
			loaded = load_stored_shape(&self.catalog, tx, &table, pre_view.fingerprint())?;
			&loaded
		};
		let previous = match pre_shape.get_value(&pre, through) {
			Value::DateTime(previous) => previous,
			other => {
				return Err(internal_error!("{} holds {:?} as complete_through", table.name, other));
			}
		};
		if at < previous {
			return Ok(Advanced::Backwards(previous));
		}

		row.set_timestamps(pre_view.created_at(), now);
		tx.update_table(&table, &[row_number], &[], &mut [row], &key_columns, from_ref(&pre), &self.catalog)?;
		Ok(Advanced::Forward)
	}
}

fn column_index(table: &Table, column: ColumnId) -> Result<usize> {
	table.columns
		.iter()
		.position(|c| c.id == column)
		.ok_or_else(|| internal_error!("{} has no column {:?}", table.name, column))
}

fn indexed_row_number(table: &Table, body: &[u8]) -> Result<RowNumber> {
	let bytes: [u8; 8] = body.try_into().map_err(|_| {
		internal_error!("{} primary key entry holds {} bytes, not a row number", table.name, body.len())
	})?;
	Ok(RowNumber(u64::from_be_bytes(bytes)))
}
