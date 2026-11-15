// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{binding::BindingProtocol, vtable::VTable},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, value::value_type::ValueType};

use super::common_vtable_columns;
use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemBindingGrpc {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemBindingGrpc {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemBindingGrpc {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_bindings_grpc_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemBindingGrpc {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<(), ValueError> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>, ValueError> {
		if self.exhausted {
			return Ok(None);
		}

		let bindings: Vec<_> = CatalogStore::list_bindings_all(txn)?
			.into_iter()
			.filter(|b| matches!(b.protocol, BindingProtocol::Grpc { .. }))
			.collect();

		let mut rpc_names = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());
		let mut formats = ColumnBuilder::with_capacity(ValueType::Utf8, bindings.len());

		for b in &bindings {
			let BindingProtocol::Grpc {
				name: rpc_name,
			} = &b.protocol
			else {
				continue;
			};
			rpc_names.push(rpc_name.as_str());
			formats.push(b.format.as_str());
		}

		let mut columns = common_vtable_columns(&bindings);
		columns.extend(vec![rpc_names.finish("rpc_name"), formats.finish("format")]);

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
