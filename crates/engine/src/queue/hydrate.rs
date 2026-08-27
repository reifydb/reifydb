// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{queue::EncodedQueueRow, shape::RowShape},
};
use reifydb_core::{
	interface::{
		catalog::{id::QueueId, queue::Queue},
		store::SingleVersionGet,
	},
	internal_error,
	key::{
		EncodableKey,
		queue_schedule::QueueItemStateKey,
		row::{RowKey, RowKeyRange},
	},
};
use reifydb_transaction::{
	multi::RangeScope,
	queue::scheduling::{QueueAdmission, admit_ready_items},
	single::SingleTransaction,
	transaction::Transaction,
};
use reifydb_value::value::{identity::IdentityId, row_number::RowNumber};
use tracing::{info, instrument};

use crate::{
	Result,
	engine::StandardEngine,
	queue::partition::{ordered_by_index, placement_of},
};

const HYDRATE_BATCH: usize = 1024;

#[instrument(name = "queue::hydrate", level = "info", skip_all)]
pub fn hydrate_queues(engine: &StandardEngine) -> Result<u64> {
	let catalog = engine.catalog();
	let single = engine.single().clone();

	let mut query = engine.begin_query(IdentityId::system())?;
	let mut txn = Transaction::Query(&mut query);

	let queues = catalog.list_queues(&mut txn)?;

	let mut admitted = 0u64;
	for queue in &queues {
		admitted += hydrate_queue(&catalog, &single, &mut txn, queue)?;
	}

	info!(queues = queues.len(), items = admitted, "queue scheduling state hydrated");

	Ok(admitted)
}

fn hydrate_queue(
	catalog: &Catalog,
	single: &SingleTransaction,
	txn: &mut Transaction<'_>,
	queue: &Queue,
) -> Result<u64> {
	let ordered_by = ordered_by_index(queue)?;

	let mut pending: BTreeMap<u16, Vec<QueueAdmission>> = BTreeMap::new();
	let mut last_key: Option<EncodedKey> = None;
	let mut admitted = 0u64;

	loop {
		let mut batch: Vec<(RowNumber, EncodedQueueRow)> = Vec::with_capacity(HYDRATE_BATCH);
		let mut fetched = 0usize;

		{
			let range = RowKeyRange::scan_range_rev(queue.id.into(), last_key.as_ref());
			let mut stream = txn.range_rev(range, RangeScope::All, HYDRATE_BATCH)?;

			for _ in 0..HYDRATE_BATCH {
				match stream.next() {
					Some(Ok(item)) => {
						fetched += 1;
						if let Some(key) = RowKey::decode(&item.key) {
							batch.push((key.row, EncodedQueueRow::from(item.bytes)));
						}
						last_key = Some(item.key);
					}
					Some(Err(err)) => return Err(err),
					None => break,
				}
			}
		}

		if !batch.is_empty() {
			let shape = load_shape(catalog, txn, queue, &batch[0].1)?;
			let store = single.read_store();

			for (row_number, encoded) in &batch {
				let placement = placement_of(queue, &shape, encoded, ordered_by, *row_number);
				let state_key = QueueItemStateKey::encoded(queue.id, placement.partition, *row_number);
				if SingleVersionGet::get(&store, &state_key)?.is_some() {
					continue;
				}

				let items = pending.entry(placement.partition).or_default();
				items.push(QueueAdmission {
					row: *row_number,
					key_hash: placement.key_hash,
					not_before: encoded.not_before(),
				});

				if items.len() >= HYDRATE_BATCH {
					admitted += flush(single, queue.id, placement.partition, items)?;
				}
			}
		}

		if fetched < HYDRATE_BATCH {
			break;
		}
	}

	for (partition, items) in pending.iter_mut() {
		admitted += flush(single, queue.id, *partition, items)?;
	}

	Ok(admitted)
}

fn flush(single: &SingleTransaction, queue: QueueId, partition: u16, items: &mut Vec<QueueAdmission>) -> Result<u64> {
	let admitted = admit_ready_items(single, queue, partition, items)?;
	items.clear();
	Ok(admitted)
}

fn load_shape(catalog: &Catalog, txn: &mut Transaction<'_>, queue: &Queue, row: &EncodedQueueRow) -> Result<RowShape> {
	let fingerprint = row.fingerprint();
	catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
		internal_error!("RowShape with fingerprint {:?} not found for queue {}", fingerprint, queue.name)
	})
}
