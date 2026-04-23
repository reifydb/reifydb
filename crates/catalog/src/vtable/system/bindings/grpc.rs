// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{binding::BindingProtocol, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::common_vtable_columns;
use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes gRPC bindings.
pub struct SystemBindingsGrpc {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemBindingsGrpc {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemBindingsGrpc {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_bindings_grpc_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemBindingsGrpc {
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
			.filter(|b| matches!(b.protocol, BindingProtocol::Grpc { .. }))
			.collect();

		let mut rpc_names = ColumnBuffer::utf8_with_capacity(bindings.len());
		let mut formats = ColumnBuffer::utf8_with_capacity(bindings.len());

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
		columns.extend(vec![
			ColumnWithName::new(Fragment::internal("rpc_name"), rpc_names),
			ColumnWithName::new(Fragment::internal("format"), formats),
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
