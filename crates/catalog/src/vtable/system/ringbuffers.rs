// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

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

		let ringbuffers = CatalogStore::list_ringbuffers(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, ringbuffers.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, ringbuffers.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, ringbuffers.len());
		let mut capacities = ColumnBuilder::with_capacity(ValueType::Uint8, ringbuffers.len());
		let mut primary_keys = ColumnBuilder::with_capacity(ValueType::Uint8, ringbuffers.len());
		let mut times = ColumnBuilder::with_capacity(ValueType::Utf8, ringbuffers.len());
		let mut timestamps = ColumnBuilder::with_capacity(ValueType::Utf8, ringbuffers.len());

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
					.unwrap_or(Value::none_of(ValueType::Uint8)),
			);
			times.push(ringbuffer.time.domain().as_str());
			timestamps.push(ringbuffer.time.ts().unwrap_or_default());
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("capacity"), capacities.finish()),
			ColumnWithName::new(Fragment::internal("primary_key_id"), primary_keys.finish()),
			ColumnWithName::new(Fragment::internal("time"), times.finish()),
			ColumnWithName::new(Fragment::internal("ts"), timestamps.finish()),
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
