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
	vtable::{
		BaseVTable, Batch, VTableContext,
		system::queue_stats::{earliest, partition_stats},
	},
};

pub struct SystemQueues {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemQueues {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemQueues {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_queues_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemQueues {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let queues = CatalogStore::list_queues(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut namespaces = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut names = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut partitions = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut ordered_by = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut deduplicate_by = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut deduplicate_ttl = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut times = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut timestamps = ColumnBuilder::with_capacity(ValueType::Utf8, queues.len());
		let mut depths = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut in_flights = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut blocked_keys = ColumnBuilder::with_capacity(ValueType::Uint8, queues.len());
		let mut oldest_due_at = ColumnBuilder::with_capacity(ValueType::DateTime, queues.len());

		for queue in queues {
			ids.push(queue.id.0);
			namespaces.push(queue.namespace.0);
			names.push(queue.name.as_str());
			partitions.push(queue.partitions() as u64);
			ordered_by.push_value(
				queue.ordered_by()
					.map(|column| Value::Utf8(column.to_string()))
					.unwrap_or(Value::none_of(ValueType::Utf8)),
			);
			match &queue.deduplicate {
				Some(deduplicate) => {
					deduplicate_by.push_value(Value::Utf8(deduplicate.by.join(",")));
					deduplicate_ttl.push_value(Value::Utf8(if deduplicate.is_forever() {
						"forever".to_string()
					} else {
						deduplicate.ttl.to_string()
					}));
				}
				None => {
					deduplicate_by.push_value(Value::none_of(ValueType::Utf8));
					deduplicate_ttl.push_value(Value::none_of(ValueType::Utf8));
				}
			}
			times.push(queue.time.domain().as_str());
			timestamps.push(queue.time.ts().unwrap_or_default());

			match partition_stats(txn, &queue)? {
				Some(stats) => {
					let mut depth = 0u64;
					let mut in_flight = 0u64;
					let mut blocked = 0u64;
					let mut oldest = None;
					for partition in stats {
						depth += partition.counters.depth;
						in_flight += partition.counters.in_flight;
						blocked += partition.counters.blocked_keys;
						oldest = earliest(oldest, partition.oldest_due_at);
					}
					depths.push_value(Value::Uint8(depth));
					in_flights.push_value(Value::Uint8(in_flight));
					blocked_keys.push_value(Value::Uint8(blocked));
					oldest_due_at.push_value(
						oldest.map(Value::DateTime)
							.unwrap_or(Value::none_of(ValueType::DateTime)),
					);
				}
				None => {
					depths.push_value(Value::none_of(ValueType::Uint8));
					in_flights.push_value(Value::none_of(ValueType::Uint8));
					blocked_keys.push_value(Value::none_of(ValueType::Uint8));
					oldest_due_at.push_value(Value::none_of(ValueType::DateTime));
				}
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces.finish()),
			ColumnWithName::new(Fragment::internal("name"), names.finish()),
			ColumnWithName::new(Fragment::internal("partitions"), partitions.finish()),
			ColumnWithName::new(Fragment::internal("ordered_by"), ordered_by.finish()),
			ColumnWithName::new(Fragment::internal("deduplicate_by"), deduplicate_by.finish()),
			ColumnWithName::new(Fragment::internal("deduplicate_ttl"), deduplicate_ttl.finish()),
			ColumnWithName::new(Fragment::internal("time"), times.finish()),
			ColumnWithName::new(Fragment::internal("ts"), timestamps.finish()),
			ColumnWithName::new(Fragment::internal("depth"), depths.finish()),
			ColumnWithName::new(Fragment::internal("in_flight"), in_flights.finish()),
			ColumnWithName::new(Fragment::internal("blocked_keys"), blocked_keys.finish()),
			ColumnWithName::new(Fragment::internal("oldest_due_at"), oldest_due_at.finish()),
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
