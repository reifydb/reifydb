// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{RowChange, RowKey, TableDef, TableRowInsertion},
	value::encoded::EncodedValues,
};
use reifydb_transaction::interceptor::TableInterceptor;
use reifydb_type::RowNumber;

use crate::StandardCommandTransaction;

pub(crate) trait TableOperations {
	async fn insert_table(
		&mut self,
		table: TableDef,
		row: EncodedValues,
		row_number: RowNumber,
	) -> crate::Result<()>;

	async fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()>;

	async fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()>;
}

impl TableOperations for StandardCommandTransaction {
	async fn insert_table(
		&mut self,
		table: TableDef,
		row: EncodedValues,
		row_number: RowNumber,
	) -> crate::Result<()> {
		TableInterceptor::pre_insert(self, &table, row_number, &row).await?;

		self.set(&RowKey::encoded(table.id, row_number), row.clone()).await?;

		TableInterceptor::post_insert(self, &table, row_number, &row).await?;

		// Track insertion for post-commit event emission
		self.track_row_change(RowChange::TableInsert(TableRowInsertion {
			table_id: table.id,
			row_number,
			encoded: row,
		}));

		Ok(())
	}

	async fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		// Get the current encoded before updating (for post-update
		// interceptor) let old_row = self.get(&key).await?.map(|v|
		// v.into());

		TableInterceptor::pre_update(self, &table, id, &row).await?;

		self.set(&key, row.clone()).await?;

		// if let Some(ref old) = old_row {
		// 	TableInterceptor::post_update(self, &table, id, &encoded, old).await?;
		// }

		// self.add_pending(PendingWrite::TableUpdate {
		// 	table,
		// 	id,
		// 	encoded,
		// });

		Ok(())
	}

	async fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()> {
		let key = RowKey::encoded(table.id, id);

		// Get the encoded before removing (for post-delete interceptor)
		// let deleted_row = self.get(&key).await?.map(|v| v.into_row());

		// Execute pre-delete interceptors
		TableInterceptor::pre_delete(self, &table, id).await?;

		// Remove the encoded from the database
		self.remove(&key).await?;

		// Execute post-delete interceptors if we had a encoded
		// if let Some(ref encoded) = deleted_row {
		// 	TableInterceptor::post_delete(self, &table, id, encoded).await?;
		// }

		// Track the removal for flow processing
		// self.add_pending(PendingWrite::TableRemove {
		// 	table,
		// 	id,
		// });

		Ok(())
	}
}
