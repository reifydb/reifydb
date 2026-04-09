// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::namespace::NamespaceToCreate;
use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, value::identity::IdentityId};
use tracing::info;

use crate::schema;

/// Bootstrap the `system::metrics` namespace and its ring buffers.
///
/// Idempotent: skips creation if the namespace or ring buffers already exist.
pub fn bootstrap_metric_ringbuffers(engine: &StandardEngine) -> Result<()> {
	let catalog = engine.catalog();
	let mut admin = engine.begin_admin(IdentityId::system())?;

	// Find or create the system::metrics namespace
	let ns_id = match catalog.find_namespace_by_path(&mut Transaction::Admin(&mut admin), "system::metrics")? {
		Some(ns) => ns.id(),
		None => {
			let ns = catalog.create_namespace(
				&mut admin,
				NamespaceToCreate {
					namespace_fragment: None,
					name: "system::metrics".to_string(),
					local_name: "metrics".to_string(),
					parent_id: NamespaceId::SYSTEM,
					token: None,
					grpc: None,
				},
			)?;
			info!("Created system::metrics namespace");
			ns.id()
		}
	};

	// Create request_history ring buffer if it doesn't exist
	if catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "request_history")?.is_none() {
		catalog.create_ringbuffer(&mut admin, schema::request_history(ns_id))?;
		info!("Created system::metrics::request_history ring buffer");
	}

	// Create statement_stats ring buffer if it doesn't exist
	if catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut admin), ns_id, "statement_stats")?.is_none() {
		catalog.create_ringbuffer(&mut admin, schema::statement_stats(ns_id))?;
		info!("Created system::metrics::statement_stats ring buffer");
	}

	admin.commit()?;

	Ok(())
}
