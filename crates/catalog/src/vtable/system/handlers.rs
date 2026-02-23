// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system handler information
pub struct Handlers {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Handlers {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_handlers_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Handlers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let handlers = CatalogStore::list_all_handlers(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(handlers.len());
		let mut namespaces = ColumnData::uint8_with_capacity(handlers.len());
		let mut names = ColumnData::utf8_with_capacity(handlers.len());
		let mut on_sumtype_ids = ColumnData::uint8_with_capacity(handlers.len());
		let mut on_variant_tags = ColumnData::uint1_with_capacity(handlers.len());

		for h in handlers {
			ids.push(h.id.0);
			namespaces.push(h.namespace.0);
			names.push(h.name.as_str());
			on_sumtype_ids.push(h.on_sumtype_id.0);
			on_variant_tags.push(h.on_variant_tag);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
			Column {
				name: Fragment::internal("on_sumtype_id"),
				data: on_sumtype_ids,
			},
			Column {
				name: Fragment::internal("on_variant_tag"),
				data: on_variant_tags,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
