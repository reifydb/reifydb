// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{sumtype::SumTypeKind, vtable::VTableDef},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system tag (sumtype) information
pub struct Tags {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl Tags {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_tags_table_def().clone(),
			exhausted: false,
		}
	}
}

impl VTable for Tags {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let sumtypes: Vec<_> = CatalogStore::list_all_sumtypes(txn)?
			.into_iter()
			.filter(|st| st.kind == SumTypeKind::Tag)
			.collect();

		let mut ids = ColumnData::uint8_with_capacity(sumtypes.len());
		let mut namespaces = ColumnData::uint8_with_capacity(sumtypes.len());
		let mut names = ColumnData::utf8_with_capacity(sumtypes.len());

		for st in sumtypes {
			ids.push(st.id.0);
			namespaces.push(st.namespace.0);
			names.push(st.name.as_str());
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
