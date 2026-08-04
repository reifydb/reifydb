// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::id::QueueId;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

use crate::engine::StandardEngine;

pub fn find_queue_id(engine: &StandardEngine, identity: IdentityId, qualified_name: &str) -> Option<QueueId> {
	let (namespace_name, queue_name) = Catalog::split_qualified_name(qualified_name)?;

	let mut query_txn = engine.begin_query(identity).ok()?;
	let mut txn = Transaction::Query(&mut query_txn);

	let catalog = engine.catalog();
	let namespace = catalog.find_namespace_by_name(&mut txn, &namespace_name).ok()??;
	let queue = catalog.find_queue_by_name(&mut txn, namespace.id(), queue_name).ok()??;

	Some(queue.id)
}
