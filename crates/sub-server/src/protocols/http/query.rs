// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::interface::Identity;
use reifydb_type::diagnostic::Diagnostic;

use crate::{
	core::{Connection, connection::RequestType, request::QueryTask},
	protocols::{
		convert::convert_params,
		ws::{ErrorResponse, QueryRequest, QueryResponse},
	},
};

/// Result of handling a query - either immediate response or pending
pub enum QueryHandlerResult {
	/// Response is ready immediately (sync execution)
	Immediate(Result<QueryResponse, ErrorResponse>),
	/// Response will be available later (async execution)
	Pending,
}

/// Handle /v1/query endpoint
pub fn handle_v1_query(conn: &mut Connection, query_req: &QueryRequest) -> QueryHandlerResult {
	// Create a channel for the response
	let (tx, rx) = mpsc::channel();

	// Concatenate all statements for execution
	let query = query_req.statements.join("; ");

	// Create the identity
	let identity = Identity::System {
		id: 1,
		name: "root".to_string(),
	};

	// Convert parameters
	let params = match convert_params(&query_req.params) {
		Ok(p) => p,
		Err(_) => {
			return QueryHandlerResult::Immediate(Err(ErrorResponse {
				diagnostic: Diagnostic {
					code: "PARAM_CONVERSION_ERROR".to_string(),
					message: "Failed to convert parameters".to_string(),
					..Default::default()
				},
			}));
		}
	};

	let task = QueryTask::new("".to_string(), query, identity, params, tx);

	if let Err(e) = conn.scheduler().once(Box::new(task)) {
		return QueryHandlerResult::Immediate(Err(ErrorResponse {
			diagnostic: Diagnostic {
				code: "SCHEDULER_ERROR".to_string(),
				message: format!("Failed to submit query to worker pool: {}", e),
				..Default::default()
			},
		}));
	}

	// Store the receiver in the connection for later polling
	conn.submit_query(rx, RequestType::HttpQuery);

	QueryHandlerResult::Pending
}
