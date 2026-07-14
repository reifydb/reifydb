// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::tag::{TypeTag, ValueKind};
use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

fn type_name(ty: &ValueType) -> String {
	match ty {
		ValueType::List(_) => "list".to_string(),
		ValueType::Record(_) => "record".to_string(),
		ValueType::Tuple(_) => "tuple".to_string(),
		ValueType::Vector(_) => "vector".to_string(),
		other => other.to_string().to_lowercase(),
	}
}

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

		let mut ids = ColumnBuffer::uint1_with_capacity(ValueKind::ALL.len());
		let mut names = ColumnBuffer::utf8_with_capacity(ValueKind::ALL.len());

		for kind in ValueKind::ALL {
			let Ok(tag) = TypeTag::new(kind, 0) else {
				continue;
			};

			let Ok(ty) = tag.to_type() else {
				continue;
			};
			ids.push(kind as u8);
			names.push(type_name(&ty).as_str());
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
