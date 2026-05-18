// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod actor;
pub mod scanner;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::Ttl,
};

pub trait ListOperatorTtls: Clone + Send + Sync + 'static {
	fn list_operator_ttls(&self) -> Vec<(FlowNodeId, Ttl)>;
	fn config(&self) -> Arc<dyn GetConfig>;
}

#[derive(Debug, Default)]
pub struct OperatorScanStats {
	pub operators_scanned: u64,

	pub operators_skipped: u64,

	pub rows_expired: u64,

	pub versions_dropped: u64,

	pub bytes_discovered: HashMap<FlowNodeId, u64>,

	pub bytes_reclaimed: HashMap<FlowNodeId, u64>,
}
