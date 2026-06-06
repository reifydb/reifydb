// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::subscription::SubscriptionWatermarkRow};

use crate::{
	store::SubscriptionStore,
	tracker::{SubscriptionPositionTracker, SubscriptionSourceTracker},
};

pub(crate) fn compute_subscription_watermarks(
	source_tracker: &SubscriptionSourceTracker,
	position_tracker: &SubscriptionPositionTracker,
	store: &SubscriptionStore,
) -> Vec<SubscriptionWatermarkRow> {
	let source_versions = source_tracker.all();
	let positions = position_tracker.all();

	let mut rows = Vec::new();

	for subscription_id in store.active_subscriptions() {
		let subscription_version = positions.get(&subscription_id).copied().unwrap_or(CommitVersion(0)).0;

		for (shape_id, version) in &source_versions {
			let lag = version.0.saturating_sub(subscription_version);
			rows.push(SubscriptionWatermarkRow {
				subscription_id,
				shape_id: *shape_id,
				lag,
			});
		}
	}

	rows
}
