// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{binding::BindingProtocol, vtable::VTable},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use super::common_vtable_columns;
use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemBindingsHttp {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemBindingsHttp {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemBindingsHttp {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_bindings_http_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemBindingsHttp {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let bindings: Vec<_> = CatalogStore::list_bindings_all(txn)?
			.into_iter()
			.filter(|b| matches!(b.protocol, BindingProtocol::Http { .. }))
			.collect();

		let mut methods = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());
		let mut paths = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());
		let mut formats = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());

		for b in &bindings {
			let BindingProtocol::Http {
				method,
				path,
			} = &b.protocol
			else {
				continue;
			};
			methods.push(method.as_str());
			paths.push(path.as_str());
			formats.push(b.format.as_str());
		}

		let mut columns = common_vtable_columns(&bindings);
		columns.extend(vec![
			ColumnWithName::new(Fragment::internal("method"), methods.finish()),
			ColumnWithName::new(Fragment::internal("path"), paths.finish()),
			ColumnWithName::new(Fragment::internal("format"), formats.finish()),
		]);

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
