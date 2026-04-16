// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::column::{Column, columns::Columns, data::ColumnData},
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

		let mut ids = ColumnData::uint8_with_capacity(procs.len());
		let mut namespace_ids = ColumnData::uint8_with_capacity(procs.len());
		let mut names = ColumnData::utf8_with_capacity(procs.len());
		let mut return_types = ColumnData::utf8_with_capacity(procs.len());
		let mut bodies = ColumnData::utf8_with_capacity(procs.len());

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
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespace_ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("return_type"),
				data: return_types,
			},
			Column {
				name: Fragment::internal("body"),
				data: bodies,
			},
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
