// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use reifydb_client::{Type, Value};

use super::{SubscriptionTestHarness, TestContext, find_column, get_op_value};

#[test]
fn test_recv_insert_notification() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("insert", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 1, name: 'test' }").await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &frames[0];

		// Verify _op column indicates INSERT (1)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(1), "_op should be 1 for INSERT");

		// Verify data columns
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.get_value(0), Value::Int4(1));

		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("test".to_string()));

		Ok(())
	});
}

#[test]
fn test_recv_update_notification() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("update", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert initial data (will receive INSERT notification)
		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		// Receive INSERT notification first
		let insert_frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_frames[0], 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Update data
		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		// Receive UPDATE notification
		let update_frames = TestContext::recv(&mut sub).await.expect("Should receive update notification");
		let frame = &update_frames[0];

		// Verify _op column indicates UPDATE (2)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(2), "_op should be 2 for UPDATE");

		// Verify updated name
		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("alice_updated".to_string()));

		Ok(())
	});
}

#[test]
fn test_recv_delete_notification() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("delete", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert initial data (will receive INSERT notification)
		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		// Receive INSERT notification first
		let insert_frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_frames[0], 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Delete data
		ctx.delete(&table, "id == 1").await?;

		// Receive DELETE notification
		let delete_frames = TestContext::recv(&mut sub).await.expect("Should receive delete notification");
		let frame = &delete_frames[0];

		// Verify _op column indicates DELETE (3)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(3), "_op should be 3 for DELETE");

		Ok(())
	});
}

#[test]
fn test_recv_multiple_rows() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("multi_rows", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert multiple rows at once
		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }, { id: 3, name: 'charlie' }")
			.await?;

		// Receive change
		let frames = TestContext::recv(&mut sub).await.expect("Should receive batch notification");

		// Verify all 3 rows are in the change
		let id_col = find_column(&frames[0], "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 3, "Should have 3 rows");

		Ok(())
	});
}

#[test]
fn test_recv_preserves_data_types() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("types", "id: int4, value: int8, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 42, value: 9999999999, name: 'test' }").await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive notification");
		let frame = &frames[0];

		// Verify types are preserved
		let id_col = find_column(frame, "id").unwrap();
		assert_eq!(id_col.data.get_value(0), Value::Int4(42));
		assert_eq!(id_col.data.get_type(), Type::Int4, "id should be Int4");

		let value_col = find_column(frame, "value").unwrap();
		assert_eq!(value_col.data.get_value(0), Value::Int8(9999999999));
		assert_eq!(value_col.data.get_type(), Type::Int8, "value should be Int8");

		let name_col = find_column(frame, "name").unwrap();
		assert_eq!(name_col.data.get_value(0), Value::Utf8("test".to_string()));
		assert_eq!(name_col.data.get_type(), Type::Utf8, "name should be Utf8");

		Ok(())
	});
}
