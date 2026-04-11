// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes system ring buffer information
pub struct SystemRingBuffers {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemRingBuffers {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemRingBuffers {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_ringbuffers_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemRingBuffers {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let ringbuffers: Vec<_> =
			CatalogStore::list_ringbuffers_all(txn)?.into_iter().filter(|rb| !rb.underlying).collect();

		let mut ids = ColumnData::uint8_with_capacity(ringbuffers.len());
		let mut namespaces = ColumnData::uint8_with_capacity(ringbuffers.len());
		let mut names = ColumnData::utf8_with_capacity(ringbuffers.len());
		let mut capacities = ColumnData::uint8_with_capacity(ringbuffers.len());
		let mut primary_keys = ColumnData::uint8_with_capacity(ringbuffers.len());

		for ringbuffer in ringbuffers {
			ids.push(ringbuffer.id.0);
			namespaces.push(ringbuffer.namespace.0);
			names.push(ringbuffer.name.as_str());
			capacities.push(ringbuffer.capacity);
			primary_keys.push_value(
				ringbuffer
					.primary_key
					.map(|pk| pk.id.0)
					.map(Value::Uint8)
					.unwrap_or(Value::none_of(Type::Uint8)),
			);
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
				name: Fragment::internal("capacity"),
				data: capacities,
			},
			Column {
				name: Fragment::internal("primary_key_id"),
				data: primary_keys,
			},
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
