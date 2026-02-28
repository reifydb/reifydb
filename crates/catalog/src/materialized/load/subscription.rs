// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			id::{PrimaryKeyId, SubscriptionId},
			key::PrimaryKeyDef,
			subscription::SubscriptionDef,
		},
		store::MultiVersionValues,
	},
	key::subscription::SubscriptionKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::subscription::schema::subscription::{self, ACKNOWLEDGED_VERSION, ID, PRIMARY_KEY},
};

pub(crate) fn load_subscriptions(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = SubscriptionKey::full_scan();
	let mut stream = rx.range(range, 1024)?;

	while let Some(result) = stream.next() {
		let multi = result?;
		let version = multi.version;

		let pk_id = get_subscription_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let subscription_def = convert_subscription(multi, primary_key);

		catalog.set_subscription(subscription_def.id, version, Some(subscription_def));
	}

	Ok(())
}

fn convert_subscription(multi: MultiVersionValues, primary_key: Option<PrimaryKeyDef>) -> SubscriptionDef {
	let row = multi.values;
	let id = SubscriptionId(subscription::SCHEMA.get_u64(&row, ID));
	let acknowledged_version = CommitVersion(subscription::SCHEMA.get_u64(&row, ACKNOWLEDGED_VERSION));

	SubscriptionDef {
		id,
		columns: vec![],
		primary_key,
		acknowledged_version,
	}
}

fn get_subscription_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = subscription::SCHEMA.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
