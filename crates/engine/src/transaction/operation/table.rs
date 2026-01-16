// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::encoded::EncodedValues, interface::catalog::table::TableDef, key::row::RowKey};
use reifydb_transaction::{
	change::{RowChange, TableRowInsertion},
	interceptor::table::TableInterceptor,
	standard::command::StandardCommandTransaction,
};
use reifydb_type::value::row_number::RowNumber;

pub(crate) trait TableOperations {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()>;

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()>;

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()>;
}

impl TableOperations for StandardCommandTransaction {
	fn insert_table(&mut self, table: TableDef, row: EncodedValues, row_number: RowNumber) -> crate::Result<()> {
		TableInterceptor::pre_insert(self, &table, row_number, &row)?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone())?;

		TableInterceptor::post_insert(self, &table, row_number, &row)?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row,
		}));

		Ok(())
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		// Get the current encoded before updating (for post-update
		// interceptor) let old_row = self.get(&key)?.map(|v|
		// v.into());

		TableInterceptor::pre_update(self, &table, id, &row)?;

		self.set(&key, row.clone())?;

		// if let Some(ref old) = old_row {
		// 	TableInterceptor::post_update(self, &table, id, &encoded, old)?;
		// }

		// self.add_pending(PendingWrite::TableUpdate {
		// 	table,
		// 	id,
		// 	encoded,
		// });

		Ok(())
	}

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		// Get the values before removing (for metrics tracking)
		let deleted_values = match self.get(&key)? {
			Some(v) => v.values,
			None => return Ok(()), // Nothing to delete
		};

		// Execute pre-delete interceptors
		TableInterceptor::pre_delete(self, &table, id)?;

		// Remove the encoded from the database
		self.unset(&key, deleted_values)?;

		Ok(())
	}
}
