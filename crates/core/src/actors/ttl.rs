// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;

/// Messages handled by the row TTL actor.
#[derive(Debug, Clone)]
pub enum RowTtlMessage {
	/// Periodic tick triggers a full scan cycle.
	Tick(DateTime),
	/// Shutdown gracefully.
	Shutdown,
}
