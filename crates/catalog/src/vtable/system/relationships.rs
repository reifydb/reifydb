// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::{batch::batch, column::factory},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemRelationships {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemRelationships {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemRelationships {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_relationships_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemRelationships {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let rels = CatalogStore::list_relationships(txn)?;
		let n = rels.len();
		let mut ids = Vec::with_capacity(n);
		let mut namespace_ids = Vec::with_capacity(n);
		let mut names = Vec::with_capacity(n);
		let mut source_table_ids = Vec::with_capacity(n);
		let mut source_column_ids = Vec::with_capacity(n);
		let mut target_table_ids = Vec::with_capacity(n);
		let mut target_column_ids = Vec::with_capacity(n);
		let mut junction_table_ids = Vec::with_capacity(n);
		let mut junction_source_column_ids = Vec::with_capacity(n);
		let mut junction_target_column_ids = Vec::with_capacity(n);
		let mut cardinalities: Vec<String> = Vec::with_capacity(n);

		for r in rels {
			ids.push(r.id.0);
			namespace_ids.push(r.namespace.0);
			names.push(r.name);
			source_table_ids.push(r.source_table.0);
			source_column_ids.push(r.source_column.0);
			target_table_ids.push(r.target_table.0);
			target_column_ids.push(r.target_column.0);
			match r.junction {
				Some(j) => {
					junction_table_ids.push(j.table.0);
					junction_source_column_ids.push(j.source_column.0);
					junction_target_column_ids.push(j.target_column.0);
				}
				None => {
					junction_table_ids.push(0);
					junction_source_column_ids.push(0);
					junction_target_column_ids.push(0);
				}
			}
			cardinalities.push(r.cardinality.as_str().to_string());
		}

		let columns = vec![
			factory::uint8("id", ids),
			factory::uint8("namespace_id", namespace_ids),
			factory::utf8("name", names),
			factory::uint8("source_table_id", source_table_ids),
			factory::uint8("source_column_id", source_column_ids),
			factory::uint8("target_table_id", target_table_ids),
			factory::uint8("target_column_id", target_column_ids),
			factory::uint8("junction_table_id", junction_table_ids),
			factory::uint8("junction_source_column_id", junction_source_column_ids),
			factory::uint8("junction_target_column_id", junction_target_column_ids),
			factory::utf8("cardinality", cardinalities),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			batch: batch(columns)?,
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
