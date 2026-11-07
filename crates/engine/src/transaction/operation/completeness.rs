// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	vm::instruction::dml::primary_key::{PrimaryKeyEncoder, get_primary_key, primary_key_index},
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
	fn advance(&self, tx: &mut Transaction<'_>, advances: &[(ObjectId, DateTime)]) -> Result<Vec<Advanced>> {
		let table = self.catalog.get_table(tx, TableId::SOURCE_COMPLETENESS)?;
		let Some(pk) = get_primary_key(&self.catalog, tx, &table)? else {
			return Err(internal_error!("{} has no primary key", table.name));
		};
		let shape = self.catalog.get_or_create_table_shape(tx, &table)?;
		let encoder = PrimaryKeyEncoder::new(&pk, &table)?;
		let key_columns = encoder.key_columns();
		let through = column_index(&table, ColumnId::SOURCE_COMPLETENESS_COMPLETE_THROUGH)?;
		let object_column = column_index(&table, ColumnId::SOURCE_COMPLETENESS_OBJECT_ID)?;

		let mut rows = Vec::with_capacity(advances.len());
		let mut index_keys = Vec::with_capacity(advances.len());
		let mut entries: Vec<TaggedKey> = Vec::with_capacity(advances.len());
		for (position, &(object, at)) in advances.iter().enumerate() {
			if advances[..position].iter().any(|&(seen, _)| seen == object) {
				return Err(internal_error!("{} advances {:?} twice in one batch", table.name, object));
			}
			let mut row = shape.allocate_table();
			shape.set_value(&mut row, object_column, &Value::Uint8(object.to_u64()));
			shape.set_value(&mut row, through, &Value::DateTime(at));
			let index_key = encoder.encode(&shape, &row);
			entries.push(IndexEntryKey::new(table.id, IndexId::primary(pk.id), index_key.clone()).into());
			index_keys.push(index_key);
			rows.push(row);
		}
		let now = self.clock.now();

		let mut existing = Vec::new();
		for (position, indexed) in tx.get(&entries)?.into_iter().enumerate() {
			if let Some(indexed) = indexed {
				let row_number =
					indexed_row_number(&table, EncodedPodRow::view(&indexed.bytes).body())?;
				existing.push((position, row_number));
			}
		}

		let mut outcomes = vec![Advanced::Inserted; advances.len()];
		let mut update_ids = Vec::with_capacity(existing.len());
		let mut update_pres = Vec::with_capacity(existing.len());
		if !existing.is_empty() {
			let row_keys: Vec<TaggedKey> = existing
				.iter()
				.map(|&(_, row_number)| row_key_from_partition(table.id, None, row_number))
				.collect();
			for (&(position, row_number), pre) in existing.iter().zip(tx.get(&row_keys)?) {
				let Some(pre) = pre else {
					return Err(internal_error!(
						"{} indexes row {:?} that does not exist",
						table.name,
						row_number
					));
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
						return Err(internal_error!(
							"{} holds {:?} as complete_through",
							table.name,
							other
						));
					}
				};
				if advances[position].1 < previous {
					outcomes[position] = Advanced::Backwards(previous);
					continue;
				}
				outcomes[position] = Advanced::Forward;
				rows[position].set_timestamps(pre_view.created_at(), now);
				update_ids.push(row_number);
				update_pres.push(pre);
			}
		}

		let mut insert_rows = Vec::new();
		let mut insert_keys = Vec::new();
		let mut update_rows = Vec::with_capacity(update_ids.len());
		for ((mut row, index_key), outcome) in rows.into_iter().zip(index_keys).zip(&outcomes) {
			match outcome {
				Advanced::Inserted => {
					row.set_timestamps(now, now);
					insert_rows.push(row);
					insert_keys.push(index_key);
				}
				Advanced::Forward => update_rows.push(row),
				Advanced::Backwards(_) => {}
			}
		}

		if !insert_rows.is_empty() {
			let ids = self.catalog.next_row_number_batch(tx, table.id, insert_rows.len() as u64)?;
			tx.insert_table(
				&table,
				&shape,
				TableInsert {
					ids: &ids,
					partitions: &[],
					rows: &mut insert_rows,
					index: primary_key_index(&table, &pk, insert_keys, &ids),
					key_columns: &key_columns,
				},
			)?;
		}
		if !update_ids.is_empty() {
			tx.update_table(
				&table,
				&update_ids,
				&[],
				&mut update_rows,
				&key_columns,
				&update_pres,
				&self.catalog,
			)?;
		}
		Ok(outcomes)
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
