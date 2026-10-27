// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{ChangeKind, SubscribeItem, SubscriptionConfig};

use super::{SubscriptionTestHarness, TestContext, find_column};

#[test]
fn a_subscription_entry_reports_per_frame_changes() {
	// The op must be derived from the frame itself, never surfaced as a column a user `_op` could shadow.
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("batch", "id: int4, name: utf8").await?;

		let rql = format!("from test::{}", table);
		let mut sub = ctx
			.client
			.subscribe(&[SubscribeItem::new(&rql, SubscriptionConfig::default())])
			.await?
			.remove(0);

		ctx.insert(&table, "{ id: 1, name: 'a' }").await?;

		let entry = TestContext::recv(&mut sub).await.expect("should receive a change before timeout");

		assert!(!entry.changes.is_empty(), "subscription entry should carry at least one frame change");
		let insert = entry
			.changes
			.iter()
			.find(|c| c.kind == ChangeKind::Insert)
			.expect("the insert should be reported with an Insert kind");

		let id = find_column(&insert.frame, "id").expect("id column should exist");
		assert_eq!(id.get_value(0), reifydb_client::Value::Int4(1));
		assert!(
			find_column(&insert.frame, "_op").is_none(),
			"the op must ride the frame, so it must never appear as a column"
		);

		Ok(())
	});
}
