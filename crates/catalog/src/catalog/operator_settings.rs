// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::OperatorSettings,
};
use reifydb_store_multi::gc::operator::ListOperatorSettings;
use reifydb_transaction::transaction::Transaction;

use crate::catalog::Catalog;

impl Catalog {
	pub fn find_operator_settings(
		&self,
		txn: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Option<OperatorSettings> {
		self.cache.find_operator_settings_at(operator, txn.version())
	}
}

impl ListOperatorSettings for Catalog {
	fn list_operator_settings(&self) -> Vec<(FlowNodeId, OperatorSettings)> {
		self.cache
			.operator_settings
			.iter()
			.filter_map(|entry| {
				let operator = *entry.key();
				let settings = entry.value().get_latest()?;
				Some((operator, settings))
			})
			.collect()
	}

	fn config(&self) -> Arc<dyn GetConfig> {
		Arc::new(self.clone())
	}
}
