// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{CatalogStore, store::subscription::SubscriptionToCreate};
use reifydb_core::{interface::CatalogTrackSubscriptionChangeOperations, value::column::Columns};
use reifydb_rql::plan::physical::CreateSubscriptionNode;
use reifydb_type::{Uuid7, Value};

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) async fn create_subscription<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateSubscriptionNode,
	) -> crate::Result<Columns> {
		let result = CatalogStore::create_subscription(
			txn,
			SubscriptionToCreate {
				columns: plan.columns,
			},
		)
		.await?;
		txn.track_subscription_def_created(result.clone())?;

		Ok(Columns::single_row([
			("subscription_id", Value::Uuid7(Uuid7(result.id.0))),
			("created", Value::Boolean(true)),
		]))
	}
}
