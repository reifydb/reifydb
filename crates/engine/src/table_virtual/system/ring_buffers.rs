// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Value};

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system ring buffer information
pub struct RingBuffers {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl RingBuffers {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_ring_buffers_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for RingBuffers {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let ring_buffers = CatalogStore::list_ring_buffers_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(ring_buffers.len());
		let mut namespaces = ColumnData::uint8_with_capacity(ring_buffers.len());
		let mut names = ColumnData::utf8_with_capacity(ring_buffers.len());
		let mut capacities = ColumnData::uint8_with_capacity(ring_buffers.len());
		let mut primary_keys = ColumnData::uint8_with_capacity(ring_buffers.len());

		for ring_buffer in ring_buffers {
			ids.push(ring_buffer.id.0);
			namespaces.push(ring_buffer.namespace.0);
			names.push(ring_buffer.name.as_str());
			capacities.push(ring_buffer.capacity);
			primary_keys.push_value(
				ring_buffer.primary_key.map(|pk| pk.id.0).map(Value::Uint8).unwrap_or(Value::Undefined),
			);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("namespace_id"),
				data: namespaces,
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: names,
			},
			Column {
				name: Fragment::owned_internal("capacity"),
				data: capacities,
			},
			Column {
				name: Fragment::owned_internal("primary_key_id"),
				data: primary_keys,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
