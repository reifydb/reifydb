// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, value_type::ValueType};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemProceduresTest {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemProceduresTest {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemProceduresTest {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_procedures_test_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemProceduresTest {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let procs: Vec<_> = CatalogStore::list_procedures_all(txn)?
			.into_iter()
			.filter(|p| matches!(p, Procedure::Test { .. }))
			.collect();

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut namespace_ids = ColumnBuilder::with_capacity(ValueType::Uint8, procs.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut return_types = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());
		let mut bodies = ColumnBuilder::with_capacity(ValueType::Utf8, procs.len());

		for p in procs {
			let Procedure::Test {
				id,
				namespace,
				name,
				return_type,
				body,
				..
			} = p
			else {
				continue;
			};
			ids.push(*id);
			namespace_ids.push(namespace.0);
			names.push(name.as_str());
			return_types.push_value(match return_type {
				Some(rt) => Value::Utf8(to_string(&rt).expect("TypeConstraint serializes")),
				None => Value::none_of(ValueType::Utf8),
			});
			bodies.push(body.as_str());
		}

		let columns = vec![
			ids.finish("id"),
			namespace_ids.finish("namespace_id"),
			names.finish("name"),
			return_types.finish("return_type"),
			bodies.finish("body"),
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
