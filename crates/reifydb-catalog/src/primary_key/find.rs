// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	QueryTransaction, TableId, TablePrimaryKeyDef, ViewId,
};

use crate::CatalogStore;

impl CatalogStore {
	pub fn find_table_primary_key(
		_rx: &mut impl QueryTransaction,
		_table: TableId,
	) -> crate::Result<Option<TablePrimaryKeyDef>> {
		// TODO: Implement primary key storage and retrieval
		// This will require:
		// 1. Storage layout for primary keys (separate table)
		// 2. Primary key creation during ALTER TABLE ADD PRIMARY KEY
		// 3. Primary key deletion during ALTER TABLE DROP PRIMARY KEY
		// For now, return None as primary keys are not yet persisted
		Ok(None)
	}

	pub fn find_view_primary_key(
		_rx: &mut impl QueryTransaction,
		_view: ViewId,
	) -> crate::Result<Option<TablePrimaryKeyDef>> {
		// TODO: Implement primary key storage and retrieval for views
		// This will require:
		// 1. Storage layout for view primary keys (separate table)
		// 2. Primary key creation during ALTER VIEW ADD PRIMARY KEY
		// 3. Primary key deletion during ALTER VIEW DROP PRIMARY KEY
		// For now, return None as primary keys are not yet persisted
		Ok(None)
	}
}
