// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
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

		let mut ids = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut namespaces = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut names = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut partitions = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut ordered_by = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut deduplicate_by = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut deduplicate_ttl = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut times = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut timestamps = ColumnBuffer::utf8_with_capacity(queues.len());
		let mut depths = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut in_flights = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut blocked_keys = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut oldest_due_at = ColumnBuffer::datetime_with_capacity(queues.len());

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
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("namespace_id"), namespaces),
			ColumnWithName::new(Fragment::internal("name"), names),
			ColumnWithName::new(Fragment::internal("partitions"), partitions),
			ColumnWithName::new(Fragment::internal("ordered_by"), ordered_by),
			ColumnWithName::new(Fragment::internal("deduplicate_by"), deduplicate_by),
			ColumnWithName::new(Fragment::internal("deduplicate_ttl"), deduplicate_ttl),
			ColumnWithName::new(Fragment::internal("time"), times),
			ColumnWithName::new(Fragment::internal("ts"), timestamps),
			ColumnWithName::new(Fragment::internal("depth"), depths),
			ColumnWithName::new(Fragment::internal("in_flight"), in_flights),
			ColumnWithName::new(Fragment::internal("blocked_keys"), blocked_keys),
			ColumnWithName::new(Fragment::internal("oldest_due_at"), oldest_due_at),
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
