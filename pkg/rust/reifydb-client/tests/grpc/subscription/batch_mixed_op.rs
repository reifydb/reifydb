// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{BatchItem, BatchStreamEvent, ChangeKind, SubscriptionConfig};
use reifydb_value::value::duration::Duration;
use tokio::time::timeout;

use super::{SubscriptionTestHarness, find_column};

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

		let env = loop {
			let event = timeout(Duration::from_milliseconds(5000).unwrap().to_std(), sub.recv())
				.await
				.expect("should receive a batch change before timeout")
				.expect("batch stream should not end");
			match event {
				BatchStreamEvent::Change(env) if env.entries.contains_key(&member_id) => break env,
				_ => continue,
			}
		};

		let entry = env.entries.get(&member_id).expect("member entry should be present");

		assert!(!entry.changes.is_empty(), "member entry should carry at least one frame change");
		let insert = entry
			.changes
			.iter()
			.find(|c| c.kind == ChangeKind::Insert)
			.expect("the insert should be reported with an Insert kind");

		let id = find_column(&insert.frame, "id").expect("id column should exist");
		assert_eq!(id.data.get_value(0), reifydb_client::Value::Int4(1));
		assert!(find_column(&insert.frame, "_op").is_none(), "_op column must be stripped");

		Ok(())
	});
}
