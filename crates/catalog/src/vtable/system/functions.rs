// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, value_type::ValueType};

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRow {
	pub name: String,
	pub namespace: Option<String>,
	pub kind: &'static str,
	pub min_args: u64,
	pub max_args: Option<u64>,
	pub description: Option<String>,
	pub alias_of: Option<String>,
}

pub struct SystemFunctions {
	pub(crate) vtable: Arc<VTable>,
	rows: Vec<FunctionRow>,
	exhausted: bool,
}

impl SystemFunctions {
	pub fn new(rows: Vec<FunctionRow>) -> Self {
		Self {
			vtable: SystemCatalog::get_system_functions_table().clone(),
			rows,
			exhausted: false,
		}
	}
}

fn optional_utf8(value: &Option<String>) -> Value {
	match value {
		Some(v) => Value::Utf8(v.clone()),
		None => Value::none_of(ValueType::Utf8),
	}
}

impl BaseVTable for SystemFunctions {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let len = self.rows.len();
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, len);
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Utf8, len);
		let mut kinds = ColumnBuilder::with_capacity(ValueType::Utf8, len);
		let mut min_args = ColumnBuilder::with_capacity(ValueType::Uint8, len);
		let mut max_args = ColumnBuilder::with_capacity(ValueType::Uint8, len);
		let mut descriptions = ColumnBuilder::with_capacity(ValueType::Utf8, len);
		let mut alias_ofs = ColumnBuilder::with_capacity(ValueType::Utf8, len);

		for row in &self.rows {
			names.push(row.name.as_str());
			namespaces.push_value(optional_utf8(&row.namespace));
			kinds.push(row.kind);
			min_args.push(row.min_args);
			max_args.push_value(match row.max_args {
				Some(n) => Value::Uint8(n),
				None => Value::none_of(ValueType::Uint8),
			});
			descriptions.push_value(optional_utf8(&row.description));
			alias_ofs.push_value(optional_utf8(&row.alias_of));
		}

		let columns = vec![
			names.finish("name"),
			namespaces.finish("namespace"),
			kinds.finish("kind"),
			min_args.finish("min_args"),
			max_args.finish("max_args"),
			descriptions.finish("description"),
			alias_ofs.finish("alias_of"),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
