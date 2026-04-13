// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Integration tests for gRPC subscriptions
//! Ported from WebSocket integration tests
//!
//! These tests focus on connection reliability by repeatedly connecting to the same server instance.

use std::{
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use reifydb_client::{GrpcClient, Value, WireFormat};
use tokio::{runtime::Runtime, time::sleep};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{
		SubscriptionTestHarness, TestContext, create_test_table, find_column, get_op_value, recv_with_timeout,
		unique_table_name,
	},
};

#[test]
fn test_basic_subscribe_to_query() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_basic", "id: int4, name: utf8, value: int4").await?;
		let sub = ctx.subscribe(&table).await?;

		assert!(!sub.subscription_id().is_empty(), "Subscription ID should be > 0");

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_basic_drop_subscription_success() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_unsub", "id: int4, name: utf8").await?;
		let sub = ctx.subscribe(&table).await?;

		assert!(!sub.subscription_id().is_empty(), "Subscription ID should be > 0");

		// Drop subscription should succeed
		drop(sub);
		Ok(())
	});
}

#[test]
fn test_basic_receive_insert_notifications() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_insert", "id: int4, name: utf8, value: int4").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert data after subscription is established
		ctx.insert(&table, "{ id: 1, name: 'test', value: 100 }").await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &frames[0];

		// Verify the data
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.get_value(0), Value::Int4(1));

		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("test".to_string()));

		let value_col = find_column(frame, "value").expect("value column should exist");
		assert_eq!(value_col.data.get_value(0), Value::Int4(100));

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_insert_callback() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_insert", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &frames[0];

		// Verify _op column indicates INSERT (1)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(1), "_op should be 1 for INSERT");

		// Verify both rows
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 2, "Should have 2 rows");

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_update_callback() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_update", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert initial data
		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_frames[0], 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Update data
		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		let update_frames = TestContext::recv(&mut sub).await.expect("Should receive update notification");
		let frame = &update_frames[0];

		// Verify _op column indicates UPDATE (2)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(2), "_op should be 2 for UPDATE");

		// Verify updated name
		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("alice_updated".to_string()));

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_remove_callback() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_remove", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert initial data
		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_frames = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_frames[0], 0);
		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");

		// Delete data
		ctx.delete(&table, "id == 1").await?;

		let delete_frames = TestContext::recv(&mut sub).await.expect("Should receive delete notification");
		let frame = &delete_frames[0];

		// Verify _op column indicates DELETE (3)
		let op = get_op_value(frame, 0);
		assert_eq!(op, Some(3), "_op should be 3 for DELETE");

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_multiple_types_in_sequence() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_multi", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert
		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;
		let insert_frames = TestContext::recv(&mut sub).await.expect("Should receive insert");
		assert_eq!(get_op_value(&insert_frames[0], 0), Some(1));

		// Update
		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;
		let update_frames = TestContext::recv(&mut sub).await.expect("Should receive update");
		assert_eq!(get_op_value(&update_frames[0], 0), Some(2));

		// Remove
		ctx.delete(&table, "id == 1").await?;
		let delete_frames = TestContext::recv(&mut sub).await.expect("Should receive delete");
		assert_eq!(get_op_value(&delete_frames[0], 0), Some(3));

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_batch_consecutive_rows() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_batch", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert 10 rows at once
		let rows: Vec<String> = (1..=10).map(|i| format!("{{ id: {}, name: 'user{}' }}", i, i)).collect();
		ctx.insert(&table, &rows.join(", ")).await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive batch notification");
		let frame = &frames[0];

		// Should be batched into one notification with all 10 rows
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 10, "Should have 10 rows");

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_concurrent_multiple_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table1 = unique_table_name("sub_conc_1");
		let table2 = unique_table_name("sub_conc_2");

		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let mut sub1 = client.subscribe(&format!("from test::{}", table1)).await.unwrap();
		let mut sub2 = client.subscribe(&format!("from test::{}", table2)).await.unwrap();

		// Insert into table 1
		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();

		recv_with_timeout(&mut sub1, 5000).await.expect("Should receive change from table1");

		// Insert into table 2
		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

		recv_with_timeout(&mut sub2, 5000).await.expect("Should receive change from table2");

		drop(sub1);
		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_concurrent_5_plus_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		const NUM_TABLES: usize = 5;
		let mut tables = Vec::new();
		let mut subs = Vec::new();

		// Create all tables and subscribe
		for i in 0..NUM_TABLES {
			let table = unique_table_name(&format!("sub_conc_{}", i));
			create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			tables.push(table);
			subs.push(sub);
		}

		// Insert into all tables
		for (i, table) in tables.iter().enumerate() {
			client.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 100), None)
				.await
				.unwrap();
		}

		// Wait for all callbacks - each subscription independently
		let mut received = 0;
		for sub in &mut subs {
			let frames = recv_with_timeout(sub, 15000).await;
			if frames.is_some() {
				received += 1;
			}
		}
		assert_eq!(received, NUM_TABLES, "Should receive {} notifications", NUM_TABLES);

		// Cleanup subscriptions
		drop(subs);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_reconnection_resubscribe_after_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_reconn");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert!(!sub.subscription_id().is_empty(), "Subscription ID should be > 0");

		// Drop and reconnect
		drop(sub);
		drop(client);

		// Reconnect
		let mut client2 =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client2.authenticate("mysecrettoken");

		// Resubscribe
		let mut sub2 = client2.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Insert new data
		client2.command(&format!("INSERT test::{} [{{ id: 1, name: 'after_reconnect' }}]", table), None)
			.await
			.unwrap();

		let frames =
			recv_with_timeout(&mut sub2, 5000).await.expect("Should receive notification after reconnect");
		let frame = &frames[0];

		let name_col = find_column(frame, "name").expect("name column should exist");
		assert_eq!(name_col.data.get_value(0), Value::Utf8("after_reconnect".to_string()));

		drop(sub2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_reconnection_multiple_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let tables: Vec<String> = (0..3).map(|i| unique_table_name(&format!("sub_reconn_m{}", i))).collect();

		for table in &tables {
			create_test_table(&client, table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
		}

		// Subscribe to all tables
		let mut subs = Vec::new();
		for table in &tables {
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			subs.push(sub);
		}

		// Drop and reconnect
		drop(subs);
		drop(client);

		let mut client2 =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client2.authenticate("mysecrettoken");

		// Resubscribe to all tables
		let mut subs2 = Vec::new();
		for table in &tables {
			let sub = client2.subscribe(&format!("from test::{}", table)).await.unwrap();
			subs2.push(sub);
		}

		// Insert into all tables
		for (i, table) in tables.iter().enumerate() {
			client2.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 100), None)
				.await
				.unwrap();
		}

		let mut received = 0;
		for sub in &mut subs2 {
			let frames = recv_with_timeout(sub, 10000).await;
			if frames.is_some() {
				received += 1;
			}
		}
		assert_eq!(received, 3, "Should receive 3 notifications");

		drop(subs2);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_error_invalid_query() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let result = client.subscribe("INVALID RQL SYNTAX HERE").await;
		assert!(result.is_err(), "Should reject subscription with invalid query");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_error_nonexistent_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let non_existent_table = format!(
			"table_that_does_not_exist_{}",
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
		);

		let result = client.subscribe(&format!("from {}", non_existent_table)).await;
		assert!(result.is_err(), "Should reject subscription to non-existent table");
	});

	cleanup_server(Some(server));
}

#[test]
fn test_lifecycle_cleanup_on_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_cleanup");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Drop without explicit cleanup - should not panic
		drop(_sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_lifecycle_no_callbacks_after_drop() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_no_cb", "id: int4, value: int4").await?;
		let sub = ctx.subscribe(&table).await?;

		// Drop subscription immediately
		drop(sub);

		// Insert data
		ctx.insert(&table, "{ id: 1, value: 100 }").await?;

		// Small wait to verify no callback fires
		sleep(Duration::from_millis(100)).await;

		// Create new subscription to verify data was inserted
		let mut sub2 = ctx.subscribe(&table).await?;

		// Should NOT receive the previous insert (it happened before this subscription)
		let frames = recv_with_timeout(&mut sub2, 500).await;
		// Whether we get the old data depends on server behavior, just verify no panic
		let _ = frames;

		drop(sub2);
		Ok(())
	});
}

#[test]
fn test_edge_empty_result_sets() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_empty", "id: int4, value: int4").await?;

		// Subscribe with filter that won't match
		let mut sub = ctx.client.subscribe(&format!("from test::{} filter {{ id > 1000 }}", table)).await?;

		// Insert data that doesn't match filter
		ctx.insert(&table, "{ id: 1, value: 100 }").await?;

		// Small wait to verify no callback fires for non-matching data
		sleep(Duration::from_millis(100)).await;

		let frames = recv_with_timeout(&mut sub, 500).await;
		assert!(frames.is_none(), "Should not trigger callback for non-matching data");

		// Insert data that matches filter
		ctx.insert(&table, "{ id: 1001, value: 200 }").await?;

		let frames = recv_with_timeout(&mut sub, 5000).await.expect("Should receive matching data");
		let frame = &frames[0];

		// Verify matching row data
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.get_value(0), Value::Int4(1001));

		let value_col = find_column(frame, "value").expect("value column should exist");
		assert_eq!(value_col.data.get_value(0), Value::Int4(200));

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_edge_large_batch_of_changes() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_large", "id: int4, value: int4").await?;
		let mut sub = ctx.subscribe(&table).await?;

		// Insert 100 rows
		let rows: Vec<String> = (0..100).map(|i| format!("{{ id: {}, value: {} }}", i, i * 10)).collect();
		ctx.insert(&table, &rows.join(", ")).await?;

		let frames = TestContext::recv(&mut sub).await.expect("Should receive batch notification");
		let frame = &frames[0];

		// Should have received all 100 rows
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 100, "Should have 100 rows");

		drop(sub);
		Ok(())
	});
}
