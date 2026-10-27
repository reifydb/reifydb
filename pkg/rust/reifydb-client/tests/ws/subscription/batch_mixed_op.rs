// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{ChangeKind, SubscribeItem, SubscriptionConfig, Value};

use super::{SubscriptionTestHarness, recv_with_timeout};

#[test]
fn a_subscription_entry_reports_per_frame_changes() {
	// Without the op riding the frame, a user column named `_op` would be read as the op.
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("batch", "id: int4, name: utf8").await?;

		let rql = format!("from test::{}", table);
		let mut sub = ctx
			.client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await?
			.remove(0);
		let subscription_id = sub.subscription_id().to_string();

		ctx.insert(&table, "{ id: 1, name: 'a' }").await?;

		let entry = recv_with_timeout(&mut sub, 5000).await.expect("should receive a change before timeout");
		assert_eq!(entry.subscription_id, subscription_id, "subscription entry should be present");

		assert!(!entry.changes.is_empty(), "subscription entry should carry at least one frame change");
		let insert = entry
			.changes
			.iter()
			.find(|c| c.kind == ChangeKind::Insert)
			.expect("the insert should be reported with an Insert kind");

		let id = insert.frame.column("id").expect("the id column reads").expect("id column should exist");
		assert_eq!(id.get_value(0), Value::Int4(1));
		assert!(
			insert.frame.column("_op").expect("the _op lookup reads").is_none(),
			"the op must ride the frame, so it must never appear as a column"
		);

		Ok(())
	});
}
