// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Type};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes all type information
pub struct Types {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl Types {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_types_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for Types {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		const TYPE_COUNT: usize = 27;

		let mut ids = ColumnData::uint1_with_capacity(TYPE_COUNT);
		let mut names = ColumnData::utf8_with_capacity(TYPE_COUNT);

		for i in 0..TYPE_COUNT as u8 {
			let ty = Type::from_u8(i);
			ids.push(i);
			names.push(ty.to_string().to_lowercase().as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: names,
			},
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
