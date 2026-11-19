// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::subscription::SubscriptionPositionRow};

use crate::{
	store::SubscriptionStore,
	tracker::{SubscriptionPositionTracker, SubscriptionSourceTracker},
};

pub(crate) fn compute_subscription_positions(
	source_tracker: &SubscriptionSourceTracker,
	position_tracker: &SubscriptionPositionTracker,
	store: &SubscriptionStore,
) -> Vec<SubscriptionPositionRow> {
	let source_versions = source_tracker.all();
	let positions = position_tracker.all();

	let mut rows = Vec::new();

	for subscription_id in store.active_subscriptions() {
		let subscription_version = positions.get(&subscription_id).copied().unwrap_or(CommitVersion(0)).0;

		for (object_id, version) in &source_versions {
			let lag = version.0.saturating_sub(subscription_version);
			rows.push(SubscriptionPositionRow {
				subscription_id,
				object_id: *object_id,
				lag,
			});
		}
	}

	rows
}
