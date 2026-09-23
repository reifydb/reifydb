// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::OperatorId},
	lifecycle::operator::ListOperatorRetention,
	row::OperatorRetention,
};

use crate::catalog::Catalog;

impl ListOperatorRetention for Catalog {
	fn list_operator_retention(&self) -> Vec<(OperatorId, OperatorRetention)> {
		self.cache
			.operator_retention
			.iter()
			.filter_map(|entry| {
				let operator = *entry.key();
				let retention = entry.value().get_latest()?;
				Some((operator, retention))
			})
			.collect()
	}

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
