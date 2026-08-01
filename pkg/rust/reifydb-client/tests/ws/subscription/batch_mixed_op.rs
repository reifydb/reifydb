// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{BatchItem, BatchPushEvent, ChangeKind, SubscriptionConfig, Value};
use reifydb_value::value::duration::Duration;
use tokio::time::timeout;

use super::SubscriptionTestHarness;

#[test]
fn test_batch_member_entry_reports_per_frame_changes() {
	// The batch path must derive the op per frame and strip `_op` before handing the entry to
	// the caller, exactly as the single-subscription path does.
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("batch", "id: int4, name: utf8").await?;

		let rql = format!("from test::{}", table);
		let mut sub =
			ctx.client.batch_subscribe(&[BatchItem::new(&rql, SubscriptionConfig::default())]).await?;
		let member_id = sub.members()[0].subscription_id.clone();

		ctx.insert(&table, "{ id: 1, name: 'a' }").await?;

		let payload = loop {
			let event = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), sub.recv())
				.await
				.expect("should receive a batch change before timeout")
				.expect("batch stream should not end");
			match event {
				BatchPushEvent::Change(payload)
					if payload.entries.iter().any(|e| e.subscription_id == member_id) =>
				{
					break payload;
				}
				_ => continue,
			}
		};

		let entry = payload
			.entries
			.iter()
			.find(|e| e.subscription_id == member_id)
			.expect("member entry should be present");

		assert!(!entry.changes.is_empty(), "member entry should carry at least one frame change");
		let insert = entry
			.changes
			.iter()
			.find(|c| c.kind == ChangeKind::Insert)
			.expect("the insert should be reported with an Insert kind");

		let id = insert.frame.columns.iter().find(|c| c.name == "id").expect("id column should exist");
		assert_eq!(id.data.get_value(0), Value::Int4(1));
		assert!(!insert.frame.columns.iter().any(|c| c.name == "_op"), "_op column must be stripped");

		Ok(())
	});
}
