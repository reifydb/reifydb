// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::queue::{Queue, QueuePartitionCounters},
		store::SingleVersionRangeRev,
	},
	key::queue::QueueDueKey,
};
use reifydb_transaction::{queue::scheduling::queue_partition_counters, transaction::Transaction};
use reifydb_value::value::{datetime::DateTime, partition::Partition};

use crate::Result;

pub(crate) struct PartitionStats {
	pub partition: Partition,
	pub counters: QueuePartitionCounters,
	pub oldest_due_at: Option<DateTime>,
}

pub(crate) fn partition_stats(txn: &mut Transaction<'_>, queue: &Queue) -> Result<Vec<PartitionStats>> {
	let single = txn.single();

	let store = single.read_store();
	let partitions = queue_partition_counters(single, queue.id)?;
	let mut stats = Vec::with_capacity(partitions.len());

	for (partition, counters) in partitions {
		let batch = SingleVersionRangeRev::range_rev_batch(
			&store,
			QueueDueKey::partition_scan(queue.id, partition).encode(),
			1,
		)?;
		let oldest_due_at =
			batch.items.first().and_then(|item| QueueDueKey::decode(&item.key)).map(|due| due.due);

		stats.push(PartitionStats {
			partition,
			counters,
			oldest_due_at,
		});
	}

	Ok(stats)
}

pub(crate) fn earliest(left: Option<DateTime>, right: Option<DateTime>) -> Option<DateTime> {
	match (left, right) {
		(Some(left), Some(right)) if right < left => Some(right),
		(Some(left), _) => Some(left),
		(None, right) => right,
	}
}
