// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::StandardCommandTransaction;
use reifydb_catalog::sequence::TableRowSequence;
use reifydb_core::interface::{Transaction, VersionedCommandTransaction};
use reifydb_core::{
	hook::table::{TablePostInsertHook, TablePreInsertHook},
	interface::{
		interceptor::TableInterceptor, EncodableKey, TableDef,
		TableRowKey,
	},
	row::EncodedRow,
	RowNumber,
};

pub(crate) trait TableOperations {
	fn insert_into_table(
		&mut self,
		table: TableDef,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn update_table(
		&mut self,
		table: TableDef,
		id: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn remove_from_table(
		&mut self,
		table: TableDef,
		id: RowNumber,
	) -> crate::Result<()>;
}

impl<T: Transaction> TableOperations for StandardCommandTransaction<T> {
	fn insert_into_table(
		&mut self,
		table: TableDef,
		row: EncodedRow,
	) -> crate::Result<()> {
		let row_number =
			TableRowSequence::next_row_number(self, table.id)?;

		TableInterceptor::pre_insert(self, &table, &row)?;

		// Still trigger hooks for backward compatibility
		self.hooks()
			.trigger(TablePreInsertHook {
				table: table.clone(),
				row: row.clone(),
			})
			.unwrap();

		self.set(
			&TableRowKey {
				table: table.id,
				row: row_number,
			}
			.encode(),
			row.clone(),
		)?;

		TableInterceptor::post_insert(self, &table, row_number, &row)?;

		// Still trigger hooks for backward compatibility
		self.hooks()
			.trigger(TablePostInsertHook {
				table: table.clone(),
				id: row_number,
				row: row.clone(),
			})
			.unwrap();

		// self.add_pending(TableInsert {
		// 	table,
		// 	id: row_number,
		// 	row,
		// });

		Ok(())
	}

	fn update_table(
		&mut self,
		table: TableDef,
		id: RowNumber,
		row: EncodedRow,
	) -> crate::Result<()> {
		let key = TableRowKey {
			table: table.id,
			row: id,
		}
		.encode();

		// Get the current row before updating (for post-update
		// interceptor) let old_row = self.get(&key)?.map(|v|
		// v.into());

		TableInterceptor::pre_update(self, &table, id, &row)?;

		self.set(&key, row.clone())?;

		// Execute post-update interceptors if we had an old row
		// if let Some(ref old) = old_row {
		// 	TableInterceptor::post_update(self, &table, id, &row, old)?;
		// }

		// self.add_pending(PendingWrite::TableUpdate {
		// 	table,
		// 	id,
		// 	row,
		// });

		Ok(())
	}

	fn remove_from_table(
		&mut self,
		table: TableDef,
		id: RowNumber,
	) -> crate::Result<()> {
		let key = TableRowKey {
			table: table.id,
			row: id,
		}
		.encode();

		// Get the row before removing (for post-delete interceptor)
		// let deleted_row = self.get(&key)?.map(|v| v.into_row());

		// Execute pre-delete interceptors
		TableInterceptor::pre_delete(self, &table, id)?;

		// Remove the row from the database
		self.remove(&key)?;

		// Execute post-delete interceptors if we had a row
		// if let Some(ref row) = deleted_row {
		// 	TableInterceptor::post_delete(self, &table, id, row)?;
		// }

		// Track the removal for flow processing
		// self.add_pending(PendingWrite::TableRemove {
		// 	table,
		// 	id,
		// });

		Ok(())
	}
}
