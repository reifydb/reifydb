// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::OperatorId},
	lifecycle::operator::ListOperatorHorizon,
	row::OperatorHorizon,
};

use crate::catalog::Catalog;

impl ListOperatorHorizon for Catalog {
	fn list_operator_horizon(&self) -> Vec<(OperatorId, OperatorHorizon)> {
		self.cache
			.operator_horizon
			.iter()
			.filter_map(|entry| {
				let operator = *entry.key();
				let horizon = entry.value().get_latest()?;
				Some((operator, horizon))
			})
			.collect()
	}

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
