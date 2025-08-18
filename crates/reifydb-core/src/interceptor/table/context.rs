// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	RowId,
	interface::{CommandTransaction, TableDef, Transaction},
	row::EncodedRow,
};

/// Context for table pre-insert interceptors
pub struct TablePreInsertContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub row: &'a EncodedRow,
}

impl<'a, T: Transaction> TablePreInsertContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		row: &'a EncodedRow,
	) -> Self {
		Self {
			txn,
			table,
			row,
		}
	}
}

/// Context for table post-insert interceptors
pub struct TablePostInsertContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub id: RowId,
	pub row: &'a EncodedRow,
}

impl<'a, T: Transaction> TablePostInsertContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
	) -> Self {
		Self {
			txn,
			table,
			id,
			row,
		}
	}
}

/// Context for table pre-update interceptors
pub struct TablePreUpdateContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub id: RowId,
	pub row: &'a EncodedRow,
}

impl<'a, T: Transaction> TablePreUpdateContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
	) -> Self {
		Self {
			txn,
			table,
			id,
			row,
		}
	}
}

/// Context for table post-update interceptors
pub struct TablePostUpdateContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub id: RowId,
	pub row: &'a EncodedRow,
	pub old_row: &'a EncodedRow,
}

impl<'a, T: Transaction> TablePostUpdateContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
		old_row: &'a EncodedRow,
	) -> Self {
		Self {
			txn,
			table,
			id,
			row,
			old_row,
		}
	}
}

/// Context for table pre-delete interceptors
pub struct TablePreDeleteContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub id: RowId,
}

impl<'a, T: Transaction> TablePreDeleteContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
	) -> Self {
		Self {
			txn,
			table,
			id,
		}
	}
}

/// Context for table post-delete interceptors
pub struct TablePostDeleteContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
	pub table: &'a TableDef,
	pub id: RowId,
	pub deleted_row: &'a EncodedRow,
}

impl<'a, T: Transaction> TablePostDeleteContext<'a, T> {
	pub fn new(
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		deleted_row: &'a EncodedRow,
	) -> Self {
		Self {
			txn,
			table,
			id,
			deleted_row,
		}
	}
}
