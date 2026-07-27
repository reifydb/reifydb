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

use super::CatalogCache;
use crate::{CatalogStore, Result, store::queue::shape::queue};

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
	let id = QueueId(queue::SHAPE.get_u64(&row, queue::ID));
	let namespace = NamespaceId(queue::SHAPE.get_u64(&row, queue::NAMESPACE));
	let name = queue::SHAPE.get_utf8(&row, queue::NAME).to_string();
	let partitions = queue::SHAPE.get_u16(&row, queue::PARTITIONS);

	let ordered_by_str = queue::SHAPE.get_utf8(&row, queue::ORDERED_BY);
	let ordered_by = if ordered_by_str.is_empty() {
		None
	} else {
		Some(ordered_by_str.to_string())
	};

	Queue {
		id,
		namespace,
		name,
		columns: vec![],
		partitions,
		ordered_by,
		retention: QueueRetention {
			done: queue::SHAPE.try_get_duration(&row, queue::RETENTION_DONE),
		},
		retry: QueueRetry {
			attempts: queue::SHAPE.get_u32(&row, queue::RETRY_ATTEMPTS),
			backoff: queue::SHAPE.get_duration(&row, queue::RETRY_BACKOFF),
		},
		underlying: queue::SHAPE.get_u8(&row, queue::UNDERLYING) != 0,
	}
}
