// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

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

pub struct SystemOperatorTypes {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemOperatorTypes {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemOperatorTypes {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_operator_types_table().clone(),
			exhausted: false,
		}
	}
}

const OPERATOR_TYPE_NAMES: [&str; 23] = [
	"source_inline_data",
	"source_table",
	"source_view",
	"source_flow",
	"filter",
	"map",
	"extend",
	"join",
	"aggregate",
	"append",
	"sort",
	"take",
	"distinct",
	"apply",
	"sink_subscription",
	"window",
	"source_ring_buffer",
	"source_series",
	"gate",
	"sink_table_view",
	"sink_ring_buffer_view",
	"sink_series_view",
	"lookup",
];

impl BaseVTable for SystemOperatorTypes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint1, OPERATOR_TYPE_NAMES.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, OPERATOR_TYPE_NAMES.len());

		for (i, name) in OPERATOR_TYPE_NAMES.iter().enumerate() {
			ids.push(i as u8);
			names.push(*name);
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

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::JoinType,
		flow::operator::{LookupObject, OperatorDef},
		interface::catalog::id::TableId,
		operator_with::LookupWith,
	};

	use super::OPERATOR_TYPE_NAMES;

	#[test]
	fn the_lookup_discriminator_names_the_lookup_row() {
		// The row id is the stored type byte; a shifted entry labels stored lookups as another operator.
		let lookup = OperatorDef::Lookup {
			join_type: JoinType::Inner,
			right: LookupObject::Table(TableId(1)),
			left: vec![],
			alias: None,
			with: LookupWith::default(),
		};
		assert_eq!(OPERATOR_TYPE_NAMES[lookup.discriminator() as usize], "lookup");
		assert_eq!(OPERATOR_TYPE_NAMES.len(), lookup.discriminator() as usize + 1, "lookup is the last type");
	}
}
