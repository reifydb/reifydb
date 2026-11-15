// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{change::CatalogTrackConfigChangeOperations, config::Config};
use reifydb_value::error::ValueError;

use crate::{
	change::{Change, OperationType::Update},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackConfigChangeOperations for AdminTransaction {
	fn track_config_set(&mut self, pre: Config, post: Config) -> Result<(), ValueError> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_config_change(change);
		Ok(())
	}
}
