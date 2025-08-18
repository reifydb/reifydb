// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PendingWrite::InsertIntoTable;
use reifydb_core::{
	RowId,
	interface::{
		CommandTransaction, EncodableKey, PendingWrite, TableDef,
		TableRowKey, Transaction, VersionedCommandTransaction,
	},
	row::EncodedRow,
};

use crate::sequence::TableRowSequence;

pub trait TableOperations {
	fn insert_into_table(
		&mut self,
		table: TableDef,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn update_table(
		&mut self,
		table: TableDef,
		id: RowId,
		row: EncodedRow,
	) -> crate::Result<()>;

	fn remove_from_table(
		&mut self,
		table: TableDef,
		id: RowId,
	) -> crate::Result<()>;
}

impl<T: Transaction> TableOperations for CommandTransaction<T> {
	fn insert_into_table(
		&mut self,
		table: TableDef,
		row: EncodedRow,
	) -> crate::Result<()> {
		let row_id = TableRowSequence::next_row_id(self, table.id)?;

		// self.hooks().trigger()

		self.set(
			&TableRowKey {
				table: table.id,
				row: row_id,
			}
			.encode(),
			row.clone(),
		)?;

		self.add_pending(InsertIntoTable {
			table,
			id: row_id,
			row,
		});

		Ok(())
	}

	fn update_table(
		&mut self,
		table: TableDef,
		id: RowId,
		row: EncodedRow,
	) -> crate::Result<()> {
		let key = TableRowKey {
			table: table.id,
			row: id,
		}
		.encode();

		// // Get the current row before updating (for pending change
		// // tracking)
		// let before = self.get(&key)?.map(|v|
		// v.into_row()).ok_or_else( 	|| {
		// 		reifydb_core::error::Error::new(
		// 			reifydb_core::error::ErrorKind::NotFound,
		// 			format!("Row with id {} not found in table {}", id,
		// table.name), 		)
		// 	},
		// )?;

		// Update the row in the database
		self.set(&key, row.clone())?;

		self.add_pending(PendingWrite::Update {
			table,
			id,
			row,
		});

		Ok(())
	}

	fn remove_from_table(
		&mut self,
		table: TableDef,
		id: RowId,
	) -> crate::Result<()> {
		let key = TableRowKey {
			table: table.id,
			row: id,
		}
		.encode();

		// Get the row before removing (for pending change tracking)
		// let row = self.get(&key)?.map(|v| v.into_row()).ok_or_else(
		// 	|| {
		// 		reifydb_core::error::Error::new(
		// 			reifydb_core::error::ErrorKind::NotFound,
		// 			format!("Row with id {} not found in table {}", id,
		// table.name), 		)
		// 	},
		// )?;

		// Remove the row from the database
		self.remove(&key)?;

		// Track the removal for flow processing
		self.add_pending(PendingWrite::Remove {
			table,
			id,
		});

		Ok(())
	}
}
