// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{SubscriptionDef, SubscriptionId, resolved::ResolvedSubscription};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};
use reifydb_type::{Fragment, error, internal};
use tracing::{instrument, warn};

use crate::{Catalog, CatalogStore};

impl Catalog {
	/// Find a subscription by ID
	#[instrument(name = "catalog::subscription::find", level = "trace", skip(self, txn))]
	pub fn find_subscription<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: SubscriptionId,
	) -> crate::Result<Option<SubscriptionDef>> {
		match txn.into_standard_transaction() {
			StandardTransaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, cmd.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) = CatalogStore::find_subscription(cmd, id)? {
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
			StandardTransaction::Query(qry) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, qry.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) = CatalogStore::find_subscription(qry, id)? {
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
		}
	}

	/// Get a subscription by ID, error if not found
	#[instrument(name = "catalog::subscription::get", level = "trace", skip(self, txn))]
	pub fn get_subscription<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		id: SubscriptionId,
	) -> crate::Result<SubscriptionDef> {
		self.find_subscription(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Subscription with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	/// Resolve a subscription ID to a fully resolved subscription
	#[instrument(name = "catalog::resolve::subscription", level = "trace", skip(self, txn))]
	pub fn resolve_subscription<T: IntoStandardTransaction>(
		&self,
		txn: &mut T,
		subscription_id: SubscriptionId,
	) -> crate::Result<ResolvedSubscription> {
		let subscription_def = self.get_subscription(txn, subscription_id)?;
		// Use subscription ID as identifier since subscriptions don't have names
		let subscription_ident = Fragment::internal(format!("subscription_{}", subscription_id.0));

		Ok(ResolvedSubscription::new(subscription_ident, subscription_def))
	}
}
