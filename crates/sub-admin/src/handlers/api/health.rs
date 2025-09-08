// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use reifydb_network::HttpResponse;

pub fn handle_health<T: Transaction>(
	_engine: &StandardEngine<T>,
) -> HttpResponse {
	HttpResponse::ok().with_json(
		r#"{"status":"healthy","service":"reifydb-admin","version":"0.0.1"}"#,
	)
}
