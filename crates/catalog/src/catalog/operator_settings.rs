// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::{config::GetConfig, flow::FlowNodeId},
	row::OperatorSettings,
};
use reifydb_store_multi::gc::operator::ListOperatorSettings;
use reifydb_transaction::transaction::Transaction;
use tracing::warn;

use crate::{CatalogStore, Result, catalog::Catalog};

impl Catalog {
	pub fn find_operator_settings(
		&self,
		txn: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<Option<OperatorSettings>> {
		if let Some(settings) = self.cache.find_operator_settings_at(operator, txn.version()) {
			return Ok(Some(settings));
		}
		if let Some(settings) = CatalogStore::find_operator_settings(txn, operator)? {
			warn!("operator settings for {:?} found in storage but not in CatalogCache", operator);
			return Ok(Some(settings));
		}
		Ok(None)
	}

	pub fn find_operator_settings_latest(&self, operator: FlowNodeId) -> Option<OperatorSettings> {
		self.cache.find_operator_settings(operator)
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
