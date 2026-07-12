// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{
		key::{KeySpec, TimestampPrecision},
		vtable::VTable,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemSegmentTrees {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemSegmentTrees {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemSegmentTrees {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_segment_trees_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemSegmentTrees {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let all_segment_trees: Vec<_> =
			CatalogStore::list_segment_tree_all(txn)?.into_iter().filter(|s| !s.underlying).collect();

		let mut ids = ColumnBuffer::uint8_with_capacity(all_segment_trees.len());
		let mut namespaces = ColumnBuffer::uint8_with_capacity(all_segment_trees.len());
		let mut names = ColumnBuffer::utf8_with_capacity(all_segment_trees.len());
		let mut key_columns = ColumnBuffer::utf8_with_capacity(all_segment_trees.len());
		let mut key_kinds = ColumnBuffer::utf8_with_capacity(all_segment_trees.len());
		let mut partition_bys = ColumnBuffer::utf8_with_capacity(all_segment_trees.len());
		let mut aggregates = ColumnBuffer::utf8_with_capacity(all_segment_trees.len());

		for s in all_segment_trees {
			ids.push(s.id.0);
			namespaces.push(s.namespace.0);
			names.push(s.name.as_str());
			key_columns.push(s.key.column());
			key_kinds.push(match &s.key {
				KeySpec::DateTime {
					precision,
					..
				} => match precision {
					TimestampPrecision::Second => "datetime(second)",
					TimestampPrecision::Millisecond => "datetime(millisecond)",
					TimestampPrecision::Microsecond => "datetime(microsecond)",
					TimestampPrecision::Nanosecond => "datetime(nanosecond)",
				},
				KeySpec::Integer {
					..
				} => "integer",
			});
			partition_bys.push(s.partition_by.join(","));
			aggregates.push(s.render_aggregates());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("key_column"), key_columns),
			ColumnWithName::new(Fragment::internal("key_kind"), key_kinds),
			ColumnWithName::new(Fragment::internal("partition_by"), partition_bys),
			ColumnWithName::new(Fragment::internal("aggregates"), aggregates),
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
