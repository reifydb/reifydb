// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{sumtype::SumTypeKind, vtable::VTable},
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemTags {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemTags {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemTags {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_tags_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemTags {
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
			.filter(|st| st.kind == SumTypeKind::Tag)
			.collect();

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, sumtypes.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, sumtypes.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, sumtypes.len());

		for st in sumtypes {
			ids.push(st.id.0);
			namespaces.push(st.namespace.0);
			names.push(st.name.as_str());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
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
