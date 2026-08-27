// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::queue::{Queue, QueuePartitionCounters, decode_queue_partition_counters},
		store::{SingleVersionGet, SingleVersionRangeRev},
	},
	key::{
		EncodableKey,
		queue_schedule::{QueueDueKey, QueuePartitionKey},
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::datetime::DateTime;

use crate::Result;

pub(crate) struct PartitionStats {
	pub partition: u16,
	pub counters: QueuePartitionCounters,
	pub oldest_due_at: Option<DateTime>,
}

pub(crate) fn partition_stats(txn: &mut Transaction<'_>, queue: &Queue) -> Result<Option<Vec<PartitionStats>>> {
	let Some(single) = txn.single() else {
		return Ok(None);
	};

	let store = single.read_store();
	let mut stats = Vec::with_capacity(usize::from(queue.partitions()));

	for partition in 0..queue.partitions() {
		let counters = SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue.id, partition))?
			.map(|stored| decode_queue_partition_counters(EncodedPodRow::view(&stored.bytes)))
			.unwrap_or_default();

		let batch = SingleVersionRangeRev::range_rev_batch(
			&store,
			QueueDueKey::partition_scan(queue.id, partition),
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

	Ok(Some(stats))
}

pub(crate) fn earliest(left: Option<DateTime>, right: Option<DateTime>) -> Option<DateTime> {
	match (left, right) {
		(Some(left), Some(right)) if right < left => Some(right),
		(Some(left), _) => Some(left),
		(None, right) => right,
	}
}
