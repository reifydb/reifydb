// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Metrics for the logging subsystem
#[derive(Debug, Clone)]
pub struct LoggingMetrics {
	pub buffered_count: usize,
	pub buffer_capacity: usize,
	pub buffer_utilization: usize,
	pub total_processed: u64,
	pub total_dropped: u64,
	pub is_running: bool,
}
