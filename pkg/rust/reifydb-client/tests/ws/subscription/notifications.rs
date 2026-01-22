// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use reifydb_client::Type;

use super::{SubscriptionTestHarness, find_column, get_op_value};

#[test]
fn test_recv_insert_notification() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("insert", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 1, name: 'test' }").await?;

		let change = ctx.recv().await.expect("Should receive insert notification");
		assert_eq!(change.subscription_id, sub_id);

		// Verify _op column indicates INSERT (1)
		let op = get_op_value(&change.frame, 0);
		assert_eq!(op, Some(1), "_op should be 1 for INSERT");

		// Verify data columns
		let id_col = find_column(&change.frame, "id").expect("id column should exist");
		assert_eq!(id_col.data[0], "1");

		let name_col = find_column(&change.frame, "name").expect("name column should exist");
		assert_eq!(name_col.data[0], "test");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_recv_update_notification() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("update", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table).await?;

		// Insert initial data (will receive INSERT notification)
		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		// Receive INSERT notification first
		let insert_change = ctx.recv().await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_change.frame, 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Update data
		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		// Receive UPDATE notification
		let update_change = ctx.recv().await.expect("Should receive update notification");
		assert_eq!(update_change.subscription_id, sub_id);

		// Verify _op column indicates UPDATE (2)
		let op = get_op_value(&update_change.frame, 0);
		assert_eq!(op, Some(2), "_op should be 2 for UPDATE");

		// Verify updated name
		let name_col = find_column(&update_change.frame, "name").expect("name column should exist");
		assert_eq!(name_col.data[0], "alice_updated");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_recv_delete_notification() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("delete", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table).await?;

		// Insert initial data (will receive INSERT notification)
		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		// Receive INSERT notification first
		let insert_change = ctx.recv().await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_change.frame, 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Delete data
		ctx.delete(&table, "id == 1").await?;

		// Receive DELETE notification
		let delete_change = ctx.recv().await.expect("Should receive delete notification");
		assert_eq!(delete_change.subscription_id, sub_id);

		// Verify _op column indicates DELETE (3)
		let op = get_op_value(&delete_change.frame, 0);
		assert_eq!(op, Some(3), "_op should be 3 for DELETE");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_recv_multiple_rows() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("multi_rows", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table).await?;

		// Insert multiple rows at once
		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }, { id: 3, name: 'charlie' }")
			.await?;

		// Receive change
		let change = ctx.recv().await.expect("Should receive batch notification");

		// Verify all 3 rows are in the change
		let id_col = find_column(&change.frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 3, "Should have 3 rows");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_recv_preserves_data_types() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("types", "id: int4, value: int8, name: utf8").await?;
		let sub_id = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 42, value: 9999999999, name: 'test' }").await?;

		let change = ctx.recv().await.expect("Should receive notification");

		// Verify types are preserved
		let id_col = find_column(&change.frame, "id").unwrap();
		assert_eq!(id_col.data[0], "42");
		assert_eq!(id_col.r#type, Type::Int4, "id should be Int4");

		let value_col = find_column(&change.frame, "value").unwrap();
		assert_eq!(value_col.data[0], "9999999999");
		assert_eq!(value_col.r#type, Type::Int8, "value should be Int8");

		let name_col = find_column(&change.frame, "name").unwrap();
		assert_eq!(name_col.data[0], "test");
		assert_eq!(name_col.r#type, Type::Utf8, "name should be Utf8");

		ctx.close(&sub_id).await
	});
}
