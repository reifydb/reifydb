// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;

/// Messages handled by the operator-state TTL actor.
///
/// Sibling to [`crate::actors::ttl::RowTtlMessage`], dedicated to evicting
/// expired rows under `FlowNodeStateKey` namespaces (operator state).
#[derive(Debug, Clone)]
pub enum OperatorTtlMessage {
	/// Periodic tick triggers a full scan cycle across all operators with TTL.
	Tick(DateTime),
	/// Shutdown gracefully.
	Shutdown,
}
