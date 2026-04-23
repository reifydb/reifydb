// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes all type information
pub struct SystemTypes {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemTypes {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemTypes {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_types_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemTypes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		const TYPE_COUNT: usize = 27;

		let mut ids = ColumnBuffer::uint1_with_capacity(TYPE_COUNT);
		let mut names = ColumnBuffer::utf8_with_capacity(TYPE_COUNT);

		for i in 1..=TYPE_COUNT as u8 {
			let ty = Type::from_u8(i);
			ids.push(i);
			names.push(ty.to_string().to_lowercase().as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
