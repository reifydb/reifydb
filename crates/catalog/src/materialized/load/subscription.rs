// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{
		MultiVersionValues, PrimaryKeyDef, PrimaryKeyId, SubscriptionDef, SubscriptionId, SubscriptionKey,
	},
};
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	MaterializedCatalog,
	store::subscription::layout::subscription::{self, ACKNOWLEDGED_VERSION, ID, PRIMARY_KEY},
};

pub(crate) fn load_subscriptions(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = SubscriptionKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

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
	let uuid = subscription::LAYOUT.get_uuid7(&row, ID);
	let id = SubscriptionId(uuid.into());
	let acknowledged_version = CommitVersion(subscription::LAYOUT.get_u64(&row, ACKNOWLEDGED_VERSION));

	SubscriptionDef {
		id,
		columns: vec![],
		primary_key,
		acknowledged_version,
	}
}

fn get_subscription_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = subscription::LAYOUT.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
