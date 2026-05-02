// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod actor;
pub mod scanner;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::Ttl,
};

/// Trait for providing per-operator TTL configurations to the operator-state actor.
pub trait ListOperatorTtls: Clone + Send + Sync + 'static {
	fn list_operator_ttls(&self) -> Vec<(FlowNodeId, Ttl)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}

/// Statistics from a single operator-state TTL scan cycle.
#[derive(Debug, Default)]
pub struct OperatorScanStats {
	/// Number of operators scanned for expired rows.
	pub operators_scanned: u64,
	/// Number of operators skipped (e.g. CleanupMode::Delete not supported in V1).
	pub operators_skipped: u64,
	/// Number of operator-state rows identified as expired.
	pub rows_expired: u64,
	/// Number of versioned entries physically dropped.
	pub versions_dropped: u64,
	/// Bytes discovered during scan (current version size).
	pub bytes_discovered: HashMap<FlowNodeId, u64>,
	/// Bytes reclaimed per operator (all versions dropped).
	pub bytes_reclaimed: HashMap<FlowNodeId, u64>,
}
