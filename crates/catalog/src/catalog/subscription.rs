// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			change::CatalogTrackSubscriptionChangeOperations, id::SubscriptionId,
			subscription::Subscription,
		},
		resolved::ResolvedSubscription,
	},
	internal,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{error, fragment::Fragment, value::r#type::Type};
use tracing::{instrument, warn};

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	store::subscription::create::{
		SubscriptionColumnToCreate as StoreSubscriptionColumnToCreate,
		SubscriptionToCreate as StoreSubscriptionToCreate,
	},
};

#[derive(Debug, Clone)]
pub struct SubscriptionColumnToCreate {
	pub name: String,
	pub ty: Type,
}

#[derive(Debug, Clone)]
pub struct SubscriptionToCreate {
	pub columns: Vec<SubscriptionColumnToCreate>,
}

impl From<SubscriptionColumnToCreate> for StoreSubscriptionColumnToCreate {
	fn from(col: SubscriptionColumnToCreate) -> Self {
		StoreSubscriptionColumnToCreate {
			name: col.name,
			ty: col.ty,
		}
	}
}

impl From<SubscriptionToCreate> for StoreSubscriptionToCreate {
	fn from(to_create: SubscriptionToCreate) -> Self {
		StoreSubscriptionToCreate {
			columns: to_create.columns.into_iter().map(|c| c.into()).collect(),
		}
	}
}

impl Catalog {
	/// Find a subscription by ID
	#[instrument(name = "catalog::subscription::find", level = "trace", skip(self, txn))]
	pub fn find_subscription(&self, txn: &mut Transaction<'_>, id: SubscriptionId) -> Result<Option<Subscription>> {
		match txn.reborrow() {
			Transaction::Command(cmd) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, cmd.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) =
					CatalogStore::find_subscription(&mut Transaction::Command(&mut *cmd), id)?
				{
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
			Transaction::Admin(admin) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, admin.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) =
					CatalogStore::find_subscription(&mut Transaction::Admin(&mut *admin), id)?
				{
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
			Transaction::Query(qry) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, qry.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) =
					CatalogStore::find_subscription(&mut Transaction::Query(&mut *qry), id)?
				{
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
			Transaction::Subscription(sub) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, sub.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) =
					CatalogStore::find_subscription(&mut Transaction::Subscription(&mut *sub), id)?
				{
					warn!(
						"Subscription with ID {:?} found in storage but not in MaterializedCatalog",
						id
					);
					return Ok(Some(subscription));
				}

				Ok(None)
			}
			Transaction::Test(t) => {
				// 1. Check MaterializedCatalog
				if let Some(subscription) = self.materialized.find_subscription(id, t.inner.version()) {
					return Ok(Some(subscription));
				}

				// 2. Fall back to storage as defensive measure
				if let Some(subscription) =
					CatalogStore::find_subscription(&mut Transaction::Admin(&mut *t.inner), id)?
				{
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
	pub fn get_subscription(&self, txn: &mut Transaction<'_>, id: SubscriptionId) -> Result<Subscription> {
		self.find_subscription(txn, id)?.ok_or_else(|| {
			error!(internal!(
				"Subscription with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			))
		})
	}

	/// Resolve a subscription ID to a fully resolved subscription
	#[instrument(name = "catalog::resolve::subscription", level = "trace", skip(self, txn))]
	pub fn resolve_subscription(
		&self,
		txn: &mut Transaction<'_>,
		subscription_id: SubscriptionId,
	) -> Result<ResolvedSubscription> {
		let subscription = self.get_subscription(txn, subscription_id)?;
		// Use subscription ID as identifier since subscriptions don't have names
		let subscription_ident = Fragment::internal(format!("subscription_{}", subscription_id.0));

		Ok(ResolvedSubscription::new(subscription_ident, subscription))
	}

	#[instrument(name = "catalog::subscription::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_subscription(
		&self,
		txn: &mut AdminTransaction,
		to_create: SubscriptionToCreate,
	) -> Result<Subscription> {
		let subscription = CatalogStore::create_subscription(txn, to_create.into())?;
		txn.track_subscription_created(subscription.clone())?;
		Ok(subscription)
	}

	#[instrument(name = "catalog::subscription::drop", level = "debug", skip(self, txn))]
	pub fn drop_subscription(&self, txn: &mut AdminTransaction, subscription: Subscription) -> Result<()> {
		CatalogStore::drop_subscription(txn, subscription.id)?;
		txn.track_subscription_deleted(subscription)?;
		Ok(())
	}

	#[instrument(name = "catalog::subscription::list_all", level = "debug", skip(self, txn))]
	pub fn list_subscriptions_all(&self, txn: &mut Transaction<'_>) -> Result<Vec<Subscription>> {
		CatalogStore::list_subscriptions_all(txn)
	}
}
