// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{procedure::Procedure, vtable::VTable},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	catalog::Catalog,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemHandlers {
	pub(crate) vtable: Arc<VTable>,
	pub(crate) catalog: Catalog,
	exhausted: bool,
}

impl SystemHandlers {
	pub fn new(catalog: Catalog) -> Self {
		Self {
			vtable: SystemCatalog::get_system_handlers_table().clone(),
			catalog,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemHandlers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = Vec::new();
		let mut namespace_ids = Vec::new();
		let mut names = Vec::new();
		let mut sumtype_ids = Vec::new();
		let mut variant_tags = Vec::new();

		let mut collect = |proc_def: &Procedure| {
			if let Some(variant) = proc_def.event_variant()
				&& !ids.contains(&*proc_def.id())
			{
				ids.push(*proc_def.id());
				namespace_ids.push(proc_def.namespace().0);
				names.push(proc_def.name().to_string());
				sumtype_ids.push(variant.sumtype_id.0);
				variant_tags.push(variant.variant_tag);
			}
		};

		for entry in self.catalog.materialized.procedures.iter() {
			if let Some(ref proc_def) = entry.value().get_latest() {
				collect(proc_def);
			}
		}

		if let Transaction::Test(t) = txn {
			for change in &t.inner.changes.procedure {
				if let Some(proc_def) = &change.post {
					collect(proc_def);
				}
			}
		}

		let len = ids.len();
		let mut id_col = ColumnBuffer::uint8_with_capacity(len);
		let mut ns_col = ColumnBuffer::uint8_with_capacity(len);
		let mut name_col = ColumnBuffer::utf8_with_capacity(len);
		let mut sumtype_col = ColumnBuffer::uint8_with_capacity(len);
		let mut tag_col = ColumnBuffer::uint1_with_capacity(len);

		for id in &ids {
			id_col.push(*id);
		}
		for ns in &namespace_ids {
			ns_col.push(*ns);
		}
		for name in &names {
			name_col.push(name.as_str());
		}
		for st in &sumtype_ids {
			sumtype_col.push(*st);
		}
		for tag in &variant_tags {
			tag_col.push(*tag);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), id_col),
			ColumnWithName::new(Fragment::internal("namespace_id"), ns_col),
			ColumnWithName::new(Fragment::internal("name"), name_col),
			ColumnWithName::new(Fragment::internal("on_sumtype_id"), sumtype_col),
			ColumnWithName::new(Fragment::internal("on_variant_tag"), tag_col),
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
