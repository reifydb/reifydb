// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{shape::ShapeId, vtable::VTable},
	retention::{CleanupMode, RetentionStrategy},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemShapeRetentionStrategies {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemShapeRetentionStrategies {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemShapeRetentionStrategies {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_shape_retention_strategies_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemShapeRetentionStrategies {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let strategies = CatalogStore::list_shape_retention_strategies(txn)?;

		let mut ids = ColumnBuffer::uint8_with_capacity(strategies.len());
		let mut shape_types = ColumnBuffer::utf8_with_capacity(strategies.len());
		let mut strategy_types = ColumnBuffer::utf8_with_capacity(strategies.len());
		let mut cleanup_modes = ColumnBuffer::utf8_with_capacity(strategies.len());
		let mut values = ColumnBuffer::uint8_with_capacity(strategies.len());

		for entry in strategies {
			let (shape_id, shape_type) = match entry.shape {
				ShapeId::Table(id) => (id.0, "table"),
				ShapeId::View(id) => (id.0, "view"),
				ShapeId::TableVirtual(id) => (id.0, "vtable"),
				ShapeId::RingBuffer(id) => (id.0, "ringbuffer"),
				ShapeId::Dictionary(id) => (id.0, "dictionary"),
				ShapeId::Series(id) => (id.0, "series"),
			};

			ids.push(shape_id);
			shape_types.push(shape_type);

			match entry.strategy {
				RetentionStrategy::KeepForever => {
					strategy_types.push("keep_forever");
					cleanup_modes.push_value(Value::none());
					values.push_value(Value::none());
				}
				RetentionStrategy::KeepVersions {
					count,
					cleanup_mode,
				} => {
					strategy_types.push("keep_versions");
					cleanup_modes.push(match cleanup_mode {
						CleanupMode::Delete => "delete",
						CleanupMode::Drop => "drop",
					});
					values.push(count);
				}
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("shape_id"), ids),
			ColumnWithName::new(Fragment::internal("shape_type"), shape_types),
			ColumnWithName::new(Fragment::internal("strategy_type"), strategy_types),
			ColumnWithName::new(Fragment::internal("cleanup_mode"), cleanup_modes),
			ColumnWithName::new(Fragment::internal("value"), values),
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
