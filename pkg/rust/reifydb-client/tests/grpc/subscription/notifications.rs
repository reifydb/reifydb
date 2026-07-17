// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_client::{ChangeKind, SubscriptionConfig, Value, ValueType};

use super::{SubscriptionTestHarness, TestContext, find_column};

#[test]
fn test_recv_insert_notification() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("insert", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'test' }").await?;

		let change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &change.changes[0].frame;

		assert_eq!(change.changes[0].kind, ChangeKind::Insert, "kind should be Insert");

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
		let mut sub = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		let insert_change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		assert_eq!(insert_change.changes[0].kind, ChangeKind::Insert, "kind should be Insert");

		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		let update_change = TestContext::recv(&mut sub).await.expect("Should receive update notification");
		let frame = &update_change.changes[0].frame;
		assert_eq!(update_change.changes[0].kind, ChangeKind::Update, "kind should be Update");

		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("alice_updated".to_string()));

		Ok(())
	});
}

#[test]
fn test_recv_delete_notification() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("delete", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;

		let insert_change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		assert_eq!(insert_change.changes[0].kind, ChangeKind::Insert, "kind should be Insert");

		ctx.delete(&table, "id == 1").await?;

		let delete_change = TestContext::recv(&mut sub).await.expect("Should receive delete notification");
		assert_eq!(delete_change.changes[0].kind, ChangeKind::Remove, "kind should be Remove");

		Ok(())
	});
}

#[test]
fn test_recv_multiple_rows() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("multi_rows", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }, { id: 3, name: 'charlie' }")
			.await?;

		let change = TestContext::recv(&mut sub).await.expect("Should receive batch notification");

		let id_col = find_column(&change.changes[0].frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 3, "Should have 3 rows");

		Ok(())
	});
}

#[test]
fn test_recv_preserves_data_types() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("types", "id: int4, value: int8, name: utf8").await?;
		let mut sub = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 42, value: 9999999999, name: 'test' }").await?;

		let change = TestContext::recv(&mut sub).await.expect("Should receive notification");
		let frame = &change.changes[0].frame;

		let id_col = find_column(frame, "id").unwrap();
		assert_eq!(id_col.data.get_value(0), Value::Int4(42));
		assert_eq!(id_col.data.get_type(), ValueType::Int4, "id should be Int4");

		let value_col = find_column(frame, "value").unwrap();
		assert_eq!(value_col.data.get_value(0), Value::Int8(9999999999));
		assert_eq!(value_col.data.get_type(), ValueType::Int8, "value should be Int8");

		let name_col = find_column(frame, "name").unwrap();
		assert_eq!(name_col.data.get_value(0), Value::Utf8("test".to_string()));
		assert_eq!(name_col.data.get_type(), ValueType::Utf8, "name should be Utf8");

		Ok(())
	});
}
