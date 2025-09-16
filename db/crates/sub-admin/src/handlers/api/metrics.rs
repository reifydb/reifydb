// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use reifydb_network::HttpResponse;
use serde_json::json;

pub fn handle_metrics<T: Transaction>(
	_engine: &StandardEngine<T>,
) -> HttpResponse {
	// TODO: Collect actual metrics from the engine
	let metrics = json!({
		"connections": 0,
		"queries_executed": 0,
		"uptime_seconds": 0,
		"memory_usage_bytes": 0,
		"database_size_bytes": 0
	});

	HttpResponse::ok().with_json(&metrics.to_string())
}
