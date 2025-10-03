// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::sequence::RowSequence;
use reifydb_core::{
	Row,
	event::catalog::TableInsertedEvent,
	interface::{
		EncodableKey, GetEncodedRowNamedLayout, MultiVersionCommandTransaction, RowKey, TableDef,
		interceptor::TableInterceptor,
	},
	value::encoded::EncodedValues,
};
use reifydb_type::RowNumber;

use crate::StandardCommandTransaction;

pub(crate) trait TableOperations {
	fn insert_into_table(&mut self, table: TableDef, row: EncodedValues) -> crate::Result<RowNumber>;

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()>;

	fn remove_from_table(&mut self, table: TableDef, id: RowNumber) -> crate::Result<()>;
}

impl TableOperations for StandardCommandTransaction {
	fn insert_into_table(&mut self, table: TableDef, row: EncodedValues) -> crate::Result<RowNumber> {
		let row_number = RowSequence::next_row_number(self, table.id)?;

		TableInterceptor::pre_insert(self, &table, &row)?;

		self.set(
			&RowKey {
				source: table.id.into(),
				row: row_number,
			}
			.encode(),
			row.clone(),
		)?;

		TableInterceptor::post_insert(self, &table, row_number, &row)?;

		let layout = table.get_named_layout();

		self.event_bus().emit(TableInsertedEvent {
			table,
			row: Row {
				number: row_number,
				encoded: row,
				layout,
			},
		});

		Ok(row_number)
	}

	fn update_table(&mut self, table: TableDef, id: RowNumber, row: EncodedValues) -> crate::Result<()> {
		let key = RowKey {
			source: table.id.into(),
			row: id,
		}
		.encode();

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
		let key = RowKey {
			source: table.id.into(),
			row: id,
		}
		.encode();

		// Get the encoded before removing (for post-delete interceptor)
		// let deleted_row = self.get(&key)?.map(|v| v.into_row());

		// Execute pre-delete interceptors
		TableInterceptor::pre_delete(self, &table, id)?;

		// Remove the encoded from the database
		self.remove(&key)?;

		// Execute post-delete interceptors if we had a encoded
		// if let Some(ref encoded) = deleted_row {
		// 	TableInterceptor::post_delete(self, &table, id, encoded)?;
		// }

		// Track the removal for flow processing
		// self.add_pending(PendingWrite::TableRemove {
		// 	table,
		// 	id,
		// });

		Ok(())
	}
}
