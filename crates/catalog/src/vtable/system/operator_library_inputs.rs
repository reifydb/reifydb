// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::tag::type_tag_byte;
use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use super::operator_libary::OperatorLibrary;
use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemOperatorLibraryInputs {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
	operator_store: OperatorLibrary,
}

impl SystemOperatorLibraryInputs {
	pub fn new(operator_store: OperatorLibrary) -> Self {
		Self {
			vtable: SystemCatalog::get_system_operator_library_inputs_table().clone(),
			exhausted: false,
			operator_store,
		}
	}
}

impl BaseVTable for SystemOperatorLibraryInputs {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let infos = self.operator_store.list();

		let capacity: usize = infos.iter().map(|op| op.input_columns.len()).sum();

		let mut operators = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
		let mut positions = ColumnBuilder::with_capacity(ValueType::Uint1, capacity);
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);
		let mut column_types = ColumnBuilder::with_capacity(ValueType::Uint1, capacity);
		let mut descriptions = ColumnBuilder::with_capacity(ValueType::Utf8, capacity);

		for info in infos {
			for (position, col) in info.input_columns.iter().enumerate() {
				operators.push(info.operator.as_str());
				positions.push(position as u8);
				names.push(col.name.as_str());
				column_types.push(type_tag_byte(&col.field_type.get_type()));
				descriptions.push(col.description.as_str());
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("operator"), operators.finish()),
			ColumnWithName::new(Fragment::internal("position"), positions.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("type"), column_types.finish()),
			ColumnWithName::new(Fragment::internal("description"), descriptions.finish()),
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
