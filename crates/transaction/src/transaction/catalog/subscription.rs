// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSubscriptionChangeOperations, id::SubscriptionId, subscription::SubscriptionDef,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalSubscriptionChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackSubscriptionChangeOperations for AdminTransaction {
	fn track_subscription_def_created(&mut self, subscription: SubscriptionDef) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(subscription),
			op: Create,
		};
		self.changes.add_subscription_def_change(change);
		Ok(())
	}

	fn track_subscription_def_updated(&mut self, pre: SubscriptionDef, post: SubscriptionDef) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_subscription_def_change(change);
		Ok(())
	}

	fn track_subscription_def_deleted(&mut self, subscription: SubscriptionDef) -> Result<()> {
		let change = Change {
			pre: Some(subscription),
			post: None,
			op: Delete,
		};
		self.changes.add_subscription_def_change(change);
		Ok(())
	}
}

impl TransactionalSubscriptionChanges for AdminTransaction {
	fn find_subscription(&self, id: SubscriptionId) -> Option<&SubscriptionDef> {
		for change in self.changes.subscription_def.iter().rev() {
			if let Some(subscription) = &change.post {
				if subscription.id == id {
					return Some(subscription);
				}
			} else if let Some(subscription) = &change.pre {
				if subscription.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_subscription_deleted(&self, id: SubscriptionId) -> bool {
		self.changes
			.subscription_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id) == Some(id))
	}
}
