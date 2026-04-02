// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![allow(clippy::tabs_in_doc_comments)]

use tracing::info;

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
pub fn log_query(query: &str) {
	info!("Query:");
	let formatted_query = query.lines().collect::<Vec<_>>().join("\n");
	info!("{}", formatted_query);
}
