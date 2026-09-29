// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::tag::value_type_from_tag_byte;
use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::value_type::ValueType;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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

		const TYPE_COUNT: usize = 25;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint1, TYPE_COUNT);
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, TYPE_COUNT);

		for i in 1..=TYPE_COUNT as u8 {
			let ty = value_type_from_tag_byte(i);
			ids.push(i);
			names.push(ty.to_string().to_lowercase().as_str());
		}

		let columns = vec![ids.finish("id"), names.finish("name")];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
