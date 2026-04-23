// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};
use serde_json::to_string;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes RQL test procedures.
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

		let mut ids = ColumnBuffer::uint8_with_capacity(procs.len());
		let mut namespace_ids = ColumnBuffer::uint8_with_capacity(procs.len());
		let mut names = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut return_types = ColumnBuffer::utf8_with_capacity(procs.len());
		let mut bodies = ColumnBuffer::utf8_with_capacity(procs.len());

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
				None => Value::none_of(Type::Utf8),
			});
			bodies.push(body.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespace_ids),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("return_type"), return_types),
			ColumnWithName::new(Fragment::internal("body"), bodies),
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
