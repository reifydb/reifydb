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
	vtable::{BaseVTable, Batch, VTableContext, system::queue_stats::partition_stats},
};

pub struct SystemQueuePartitions {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemQueuePartitions {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemQueuePartitions {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_queue_partitions_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemQueuePartitions {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let queues: Vec<_> =
			CatalogStore::list_queues_all(txn)?.into_iter().filter(|queue| !queue.underlying).collect();

		let mut queue_ids = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut partitions = ColumnBuffer::uint2_with_capacity(queues.len());
		let mut depths = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut in_flights = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut blocked_keys = ColumnBuffer::uint8_with_capacity(queues.len());
		let mut oldest_due_at = ColumnBuffer::datetime_with_capacity(queues.len());

		for queue in queues {
			let Some(stats) = partition_stats(txn, &queue)? else {
				continue;
			};

			for partition in stats {
				queue_ids.push(queue.id.0);
				partitions.push(partition.partition);
				depths.push(partition.counters.depth);
				in_flights.push(partition.counters.in_flight);
				blocked_keys.push(partition.counters.blocked_keys);
				oldest_due_at.push_value(
					partition
						.oldest_due_at
						.map(Value::DateTime)
						.unwrap_or(Value::none_of(ValueType::DateTime)),
				);
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("queue_id"), queue_ids),
			ColumnWithName::new(Fragment::internal("partition"), partitions),
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
