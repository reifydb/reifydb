// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSystemConfigChangeOperations,
	config::{SystemConfig, SystemConfigKey},
};
use reifydb_type::Result;

use crate::{
	change::{Change, OperationType::Update, TransactionalSystemConfigChanges},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackSystemConfigChangeOperations for AdminTransaction {
	fn track_system_config_set(&mut self, pre: SystemConfig, post: SystemConfig) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_system_config_change(change);
		Ok(())
	}
}

impl TransactionalSystemConfigChanges for AdminTransaction {
	fn find_system_config(&self, key: SystemConfigKey) -> Option<&SystemConfig> {
		self.changes.system_config.iter().rev().find_map(|change| change.post.as_ref().filter(|c| c.key == key))
	}
}
