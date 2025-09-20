// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{TableVirtualDef, Transaction},
	value::columnar::{Column, ColumnData, ColumnQualified, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system primary key column relationships
pub struct PrimaryKeyColumns<T: Transaction> {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> PrimaryKeyColumns<T> {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_primary_key_columns_table_def().clone(),
			exhausted: false,
			_phantom: PhantomData,
		}
	}
}

impl<'a, T: Transaction> TableVirtual<'a, T> for PrimaryKeyColumns<T> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a, T>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a, T>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut pk_ids = Vec::new();
		let mut column_ids = Vec::new();
		let mut positions = Vec::new();

		let pk_columns = CatalogStore::list_primary_key_columns(txn)?;
		for (pk_id, column_id, position) in pk_columns {
			pk_ids.push(pk_id);
			column_ids.push(column_id);
			positions.push(position as u16);
		}

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: Fragment::owned_internal("primary_key_id"),
				data: ColumnData::uint8(pk_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: Fragment::owned_internal("column_id"),
				data: ColumnData::uint8(column_ids),
			}),
			Column::ColumnQualified(ColumnQualified {
				name: Fragment::owned_internal("position"),
				data: ColumnData::uint2(positions),
			}),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
