// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, QueueId},
			queue::{Queue, QueueDone, QueueRetry},
		},
		store::MultiVersionRow,
	},
	key::queue::QueueKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use super::CatalogCache;
use crate::{
	CatalogStore,
	store::queue::{
		decode_queue_time,
		shape::{decode_deduplicate, decode_dispatch, decode_partition_by, queue},
	},
};

pub(crate) fn load_queues(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = QueueKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut queues = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;
		queues.push((convert_queue(multi)?, version));
	}
	drop(stream);

	for (mut loaded, version) in queues {
		loaded.columns = CatalogStore::list_columns(rx, loaded.id)?;
		catalog.set_queue(loaded.id, version, Some(loaded));
	}

	Ok(())
}

fn convert_queue<K>(multi: MultiVersionRow<K>) -> Result<Queue, ValueError> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = QueueId(queue::get_id(&bytes));
	let namespace = NamespaceId(queue::get_namespace(&bytes));
	let name = queue::get_name(&bytes).to_string();

	Ok(Queue {
		id,
		namespace,
		name,
		columns: vec![],
		partition_by: decode_partition_by(&bytes),
		dispatch: decode_dispatch(&bytes),
		done: QueueDone {
			ttl: queue::try_get_done_ttl(&bytes),
		},
		retry: QueueRetry {
			attempts: queue::get_retry_attempts(&bytes),
			backoff: queue::get_retry_backoff(&bytes),
		},
		deduplicate: decode_deduplicate(&bytes),
		time: decode_queue_time(&bytes),
	})
}
