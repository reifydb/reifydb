// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use tracing::info;

/// Helper function to log queries with formatting
/// The query text is displayed in bold for better readability
pub fn log_query(query: &str) {
	info!("Query:");
	let formatted_query = query.lines().collect::<Vec<_>>().join("\n");
	info!("{}", formatted_query);
}
