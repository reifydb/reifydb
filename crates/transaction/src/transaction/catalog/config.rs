// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackConfigChangeOperations,
	config::{Config, ConfigKey},
};
use reifydb_type::Result;

use crate::{
	change::{Change, OperationType::Update, TransactionalConfigChanges},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackConfigChangeOperations for AdminTransaction {
	fn track_config_set(&mut self, pre: Config, post: Config) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_config_change(change);
		Ok(())
	}
}

impl TransactionalConfigChanges for AdminTransaction {
	fn find_config(&self, key: ConfigKey) -> Option<&Config> {
		self.changes.config.iter().rev().find_map(|change| change.post.as_ref().filter(|c| c.key == key))
	}
}
