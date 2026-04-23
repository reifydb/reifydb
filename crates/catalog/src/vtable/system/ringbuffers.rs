// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
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

		let mut ids = ColumnBuffer::uint8_with_capacity(ringbuffers.len());
		let mut namespaces = ColumnBuffer::uint8_with_capacity(ringbuffers.len());
		let mut names = ColumnBuffer::utf8_with_capacity(ringbuffers.len());
		let mut capacities = ColumnBuffer::uint8_with_capacity(ringbuffers.len());
		let mut primary_keys = ColumnBuffer::uint8_with_capacity(ringbuffers.len());

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
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("capacity"), capacities),
			ColumnWithName::new(Fragment::internal("primary_key_id"), primary_keys),
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
