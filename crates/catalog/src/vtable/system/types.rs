// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes all type information
pub struct Types {
	pub(crate) definition: Arc<VTableDef>,
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

impl VTable for Types {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		const TYPE_COUNT: usize = 27;

		let mut ids = ColumnData::uint1_with_capacity(TYPE_COUNT);
		let mut names = ColumnData::utf8_with_capacity(TYPE_COUNT);

		for i in 1..=TYPE_COUNT as u8 {
			let ty = Type::from_u8(i);
			ids.push(i);
			names.push(ty.to_string().to_lowercase().as_str());
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
