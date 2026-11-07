// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::row::{queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{
	interface::{
		catalog::{id::QueueId, queue::Queue},
		store::SingleVersionGet,
	},
	internal_error,
	key::{any::TaggedKey, queue::QueueItemStateKey, row::RowKeyRange},
};
use reifydb_runtime::context::transaction::TransactionContext;
use reifydb_transaction::{
	multi::RangeScope,
	queue::scheduling::{QueueAdmission, admit_ready_items},
	single::SingleTransaction,
	transaction::Transaction,
};
use reifydb_value::value::{identity::IdentityId, partition::Partition, row_number::RowNumber};
use tracing::{info, instrument};

use crate::{
	Result,
	engine::StandardEngine,
	queue::partition::{ordered_by_index, partition_by_indices, placement_of},
};

const HYDRATE_BATCH: usize = 1024;

#[instrument(name = "queue::hydrate", level = "info", skip_all)]
pub fn hydrate_queues(engine: &StandardEngine) -> Result<u64> {
	let catalog = engine.catalog();
	let single = engine.single().clone();
	let ctx = Arc::new(TransactionContext::system(engine.runtime_context().clone()));

	let mut query = engine.begin_query(IdentityId::system())?;
	let mut txn = Transaction::Query(&mut query);

	let queues = catalog.list_queues(&mut txn)?;

	let mut admitted = 0u64;
	for queue in &queues {
		admitted += hydrate_queue(&ctx, &catalog, &single, &mut txn, queue)?;
	}

	info!(queues = queues.len(), items = admitted, "queue scheduling state hydrated");

	Ok(admitted)
}

fn hydrate_queue(
	ctx: &Arc<TransactionContext>,
	catalog: &Catalog,
	single: &SingleTransaction,
	txn: &mut Transaction<'_>,
	queue: &Queue,
) -> Result<u64> {
	let ordered_by = ordered_by_index(queue)?;
	let partition_by = partition_by_indices(queue);

	let mut pending: BTreeMap<Partition, Vec<QueueAdmission>> = BTreeMap::new();
	let mut last_key: Option<TaggedKey> = None;
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
						if let TaggedKey::Row(key) = &item.key {
							batch.push((key.row, EncodedQueueRow::from(item.bytes)));
						}
						last_key = Some(item.key.clone());
					}
					Some(Err(err)) => return Err(err),
					None => break,
				}
			}
		}

		if !batch.is_empty() {
			let shape = load_shape(catalog, txn, queue, &batch[0].1)?;
			let store = single.read_store();

			let placements: Vec<_> = batch
				.iter()
				.map(|(_, encoded)| placement_of(&shape, encoded, &partition_by, ordered_by))
				.collect();
			let state_keys: Vec<_> = batch
				.iter()
				.zip(&placements)
				.map(|((row_number, _), placement)| {
					QueueItemStateKey::encoded(queue.id, placement.partition, *row_number)
				})
				.collect();
			let states = SingleVersionGet::get(&store, ctx, &state_keys)?;

			for (((row_number, encoded), placement), state) in batch.iter().zip(placements).zip(states) {
				if state.is_some() {
					continue;
				}

				let items = pending.entry(placement.partition).or_default();
				items.push(QueueAdmission {
					row: *row_number,
					key_hash: placement.key_hash,
					not_before: encoded.not_before(),
				});

				if items.len() >= HYDRATE_BATCH {
					admitted += flush(ctx, single, queue.id, placement.partition, items)?;
				}
			}
		}

		if fetched < HYDRATE_BATCH {
			break;
		}
	}

	for (partition, items) in pending.iter_mut() {
		admitted += flush(ctx, single, queue.id, *partition, items)?;
	}

	Ok(admitted)
}

fn flush(
	ctx: &Arc<TransactionContext>,
	single: &SingleTransaction,
	queue: QueueId,
	partition: Partition,
	items: &mut Vec<QueueAdmission>,
) -> Result<u64> {
	let admitted = admit_ready_items(ctx, single, queue, partition, items)?;
	items.clear();
	Ok(admitted)
}

fn load_shape(catalog: &Catalog, txn: &mut Transaction<'_>, queue: &Queue, row: &EncodedQueueRow) -> Result<RowShape> {
	let fingerprint = row.fingerprint();
	catalog.get_or_load_row_shape(fingerprint, txn)?.ok_or_else(|| {
		internal_error!("RowShape with fingerprint {:?} not found for queue {}", fingerprint, queue.name)
	})
}
