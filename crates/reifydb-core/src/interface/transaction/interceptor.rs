// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{TableDef, Transaction};
use crate::row::EncodedRow;
use crate::RowNumber;

pub trait TableInterceptor<T: Transaction> {
	/// Intercept table pre-insert operations
	fn pre_insert(
		&mut self,
		table: &TableDef,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table post-insert operations
	fn post_insert(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table pre-update operations
	fn pre_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table post-update operations
	fn post_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
		old_row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table pre-delete operations
	fn pre_delete(
		&mut self,
		table: &TableDef,
		id: RowNumber,
	) -> crate::Result<()>;

	/// Intercept table post-delete operations
	fn post_delete(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		deleted_row: &EncodedRow,
	) -> crate::Result<()>;
}

pub trait TransactionInterceptor<T: Transaction> {
	/// Intercept pre-commit operations
	fn pre_commit(&mut self) -> crate::Result<()>;

	/// Intercept post-commit operations
	fn post_commit(&mut self, version: crate::Version)
	-> crate::Result<()>;
}
