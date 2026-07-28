// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, QueueId},
			queue::{Queue, QueueRetention, QueueRetry},
		},
		store::MultiVersionRow,
	},
	key::queue::QueueKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::duration::Duration;

use super::CatalogCache;
use crate::{
	CatalogStore, Result,
	store::queue::{
		decode_queue_time,
		shape::{decode_deduplicate, decode_dispatch, queue},
	},
};

pub(crate) fn load_queues(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = QueueKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut queues = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;
		queues.push((convert_queue(multi), version));
	}
	drop(stream);

	for (mut loaded, version) in queues {
		loaded.columns = CatalogStore::list_columns(rx, loaded.id)?;
		catalog.set_queue(loaded.id, version, Some(loaded));
	}

	Ok(())
}

fn convert_queue(multi: MultiVersionRow) -> Queue {
	let row = multi.row;
	let id = QueueId(queue::SHAPE.get::<u64>(&row, queue::ID));
	let namespace = NamespaceId(queue::SHAPE.get::<u64>(&row, queue::NAMESPACE));
	let name = queue::SHAPE.get_utf8(&row, queue::NAME).to_string();

	Queue {
		id,
		namespace,
		name,
		columns: vec![],
		dispatch: decode_dispatch(&row),
		retention: QueueRetention {
			done: queue::SHAPE.try_get::<Duration>(&row, queue::RETENTION_DONE),
		},
		retry: QueueRetry {
			attempts: queue::SHAPE.get::<u32>(&row, queue::RETRY_ATTEMPTS),
			backoff: queue::SHAPE.get::<Duration>(&row, queue::RETRY_BACKOFF),
		},
		underlying: queue::SHAPE.get::<u8>(&row, queue::UNDERLYING) != 0,
		deduplicate: decode_deduplicate(&row),
		time: decode_queue_time(&row),
	}
}
