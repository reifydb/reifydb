// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::VTableDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, Value};

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system ring buffer information
pub struct RingBuffers {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl RingBuffers {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_ringbuffers_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl<T: IntoStandardTransaction> VTable<T> for RingBuffers {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let ringbuffers = CatalogStore::list_ringbuffers_all(txn).await?;

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
				ringbuffer.primary_key.map(|pk| pk.id.0).map(Value::Uint8).unwrap_or(Value::Undefined),
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
