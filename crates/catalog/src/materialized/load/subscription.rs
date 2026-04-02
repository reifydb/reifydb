// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			id::{PrimaryKeyId, SubscriptionId},
			key::PrimaryKey,
			subscription::Subscription,
		},
		store::MultiVersionRow,
	},
	key::subscription::SubscriptionKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::subscription::shape::subscription::{self, ACKNOWLEDGED_VERSION, ID, PRIMARY_KEY},
};

pub(crate) fn load_subscriptions(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = SubscriptionKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for result in stream {
		let multi = result?;
		let version = multi.version;

		let pk_id = get_subscription_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let subscription = convert_subscription(multi, primary_key);

		catalog.set_subscription(subscription.id, version, Some(subscription));
	}

	Ok(())
}

fn convert_subscription(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Subscription {
	let row = multi.row;
	let id = SubscriptionId(subscription::SHAPE.get_u64(&row, ID));
	let acknowledged_version = CommitVersion(subscription::SHAPE.get_u64(&row, ACKNOWLEDGED_VERSION));

	Subscription {
		id,
		columns: vec![],
		primary_key,
		acknowledged_version,
	}
}

fn get_subscription_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = subscription::SHAPE.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
