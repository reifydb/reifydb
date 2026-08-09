// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
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
		queues.push((convert_queue(multi)?, version));
	}
	drop(stream);

	for (mut loaded, version) in queues {
		loaded.columns = CatalogStore::list_columns(rx, loaded.id)?;
		catalog.set_queue(loaded.id, version, Some(loaded));
	}

	Ok(())
}

fn convert_queue(multi: MultiVersionRow) -> Result<Queue> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = QueueId(queue::get_id(&bytes));
	let namespace = NamespaceId(queue::get_namespace(&bytes));
	let name = queue::get_name(&bytes).to_string();

	Ok(Queue {
		id,
		namespace,
		name,
		columns: vec![],
		dispatch: decode_dispatch(&bytes),
		retention: QueueRetention {
			done: queue::try_get_retention_done(&bytes),
		},
		retry: QueueRetry {
			attempts: queue::get_retry_attempts(&bytes),
			backoff: queue::get_retry_backoff(&bytes),
		},
		underlying: queue::get_underlying(&bytes) != 0,
		deduplicate: decode_deduplicate(&bytes),
		time: decode_queue_time(&bytes),
	})
}
