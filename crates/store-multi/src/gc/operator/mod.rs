// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod actor;
pub mod scanner;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::OperatorSettings,
};

pub trait ListOperatorSettings: Clone + Send + Sync + 'static {
	fn list_operator_settings(&self) -> Vec<(FlowNodeId, OperatorSettings)>;
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
