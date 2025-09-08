// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::Transaction;
use reifydb_engine::StandardEngine;
use reifydb_network::{HttpRequest, HttpResponse};
use serde_json::{Value, json};

pub fn handle_execute<T: Transaction>(
	_engine: &StandardEngine<T>,
	request: HttpRequest,
) -> HttpResponse {
	// Parse JSON body
	let body_str = String::from_utf8_lossy(&request.body);
	let json_body: Result<Value, _> = serde_json::from_str(&body_str);

	match json_body {
		Ok(json) => {
			if let Some(_query) =
				json.get("query").and_then(|q| q.as_str())
			{
				// TODO: Execute query using the engine
				// For now, return a placeholder response
				let result = json!({
					"success": true,
					"message": "Query execution not yet implemented",
					"query": json.get("query")
				});
				HttpResponse::ok()
					.with_json(&result.to_string())
			} else {
				HttpResponse::bad_request().with_json(
					r#"{"error":"Missing 'query' field"}"#,
				)
			}
		}
		Err(e) => HttpResponse::bad_request().with_json(&format!(
			r#"{{"error":"Invalid JSON: {}"}}"#,
			e
		)),
	}
}
