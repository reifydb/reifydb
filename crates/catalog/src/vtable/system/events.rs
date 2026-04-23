// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{sumtype::SumTypeKind, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system event (sumtype) information
pub struct SystemEvents {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemEvents {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemEvents {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_events_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemEvents {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let sumtypes: Vec<_> = CatalogStore::list_all_sumtypes(txn)?
			.into_iter()
			.filter(|st| st.kind == SumTypeKind::Event)
			.collect();

		let mut ids = ColumnBuffer::uint8_with_capacity(sumtypes.len());
		let mut namespaces = ColumnBuffer::uint8_with_capacity(sumtypes.len());
		let mut names = ColumnBuffer::utf8_with_capacity(sumtypes.len());

		for st in sumtypes {
			ids.push(st.id.0);
			namespaces.push(st.namespace.0);
			names.push(st.name.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces),
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
