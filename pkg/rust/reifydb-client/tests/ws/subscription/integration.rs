// // SPDX-License-Identifier: MIT
// // Copyright (c) 2025 ReifyDB
//
// //! Integration tests for WebSocket subscriptions
// //! Ported from TypeScript integration tests in pkg/typescript/client/tests/integration/ws/subscription.test.ts
// //!
// //! These tests focus on connection reliability by repeatedly connecting to the same server instance.
//
// use std::collections::HashSet;
// use std::error::Error;
// use std::sync::atomic::{AtomicUsize, Ordering};
// use std::sync::Arc;
// use std::time::Duration;
//
// use tokio::runtime::Runtime;
// use tokio::time::sleep;
//
// use crate::common::{cleanup_server, create_server_instance, start_server_and_get_ws_port};
// use crate::ws::subscription::{
// 	create_test_table, find_column, get_op_value, recv_multiple_with_timeout, recv_with_timeout,
// 	unique_table_name, SubscriptionTestHarness,
// };
// use reifydb_client::WsClient;
//
// // ============================================================================
// // Basic Subscription Flow
// // ============================================================================
//
// #[test]
// fn test_basic_subscribe_to_query() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_basic", "id: int4, name: utf8, value: int4").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		assert!(!sub_id.is_empty(), "Subscription ID should be defined");
// 		assert!(sub_id.len() > 0, "Subscription ID should have length > 0");
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// fn test_basic_unsubscribe_success() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_unsub", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		assert!(!sub_id.is_empty(), "Subscription ID should be defined");
//
// 		// Unsubscribe should succeed
// 		ctx.client.unsubscribe(&sub_id).await?;
// 		ctx.client.close().await?;
// 		Ok(())
// 	});
// }
//
// #[test]
// fn test_basic_receive_insert_notifications() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_insert", "id: int4, name: utf8, value: int4").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert data after subscription is established
// 		ctx.insert(&table, "{ id: 1, name: 'test', value: 100 }").await?;
//
// 		let change = ctx.recv().await.expect("Should receive insert notification");
//
// 		// Verify the data
// 		let id_col = find_column(&change.frame, "id").expect("id column should exist");
// 		assert_eq!(id_col.data[0], "1");
//
// 		let name_col = find_column(&change.frame, "name").expect("name column should exist");
// 		assert_eq!(name_col.data[0], "test");
//
// 		let value_col = find_column(&change.frame, "value").expect("value column should exist");
// 		assert_eq!(value_col.data[0], "100");
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// // ============================================================================
// // Operation Callbacks (Insert, Update, Remove)
// // ============================================================================
//
// #[test]
// fn test_op_insert_callback() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_op_insert", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
//
// 		let change = ctx.recv().await.expect("Should receive insert notification");
// 		assert_eq!(change.subscription_id, sub_id);
//
// 		// Verify _op column indicates INSERT (1)
// 		let op = get_op_value(&change.frame, 0);
// 		assert_eq!(op, Some(1), "_op should be 1 for INSERT");
//
// 		// Verify both rows
// 		let id_col = find_column(&change.frame, "id").expect("id column should exist");
// 		assert_eq!(id_col.data.len(), 2, "Should have 2 rows");
//
// 		let name_col = find_column(&change.frame, "name").expect("name column should exist");
// 		assert!(name_col.data.contains(&"alice".to_string()));
// 		assert!(name_col.data.contains(&"bob".to_string()));
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// fn test_op_update_callback() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_op_update", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert initial data
// 		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
// 		let insert_change = ctx.recv().await.expect("Should receive insert notification");
// 		let insert_op = get_op_value(&insert_change.frame, 0);
// 		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");
//
// 		// Update data
// 		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;
//
// 		let update_change = ctx.recv().await.expect("Should receive update notification");
// 		assert_eq!(update_change.subscription_id, sub_id);
//
// 		// Verify _op column indicates UPDATE (2)
// 		let op = get_op_value(&update_change.frame, 0);
// 		assert_eq!(op, Some(2), "_op should be 2 for UPDATE");
//
// 		// Verify updated name
// 		let name_col = find_column(&update_change.frame, "name").expect("name column should exist");
// 		assert_eq!(name_col.data[0], "alice_updated");
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// fn test_op_remove_callback() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_op_remove", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert initial data
// 		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
// 		let insert_change = ctx.recv().await.expect("Should receive insert notification");
// 		let insert_op = get_op_value(&insert_change.frame, 0);
// 		assert_eq!(insert_op, Some(1), "_op should be 1 for INSERT");
//
// 		// Delete data
// 		ctx.delete(&table, "id == 1").await?;
//
// 		let delete_change = ctx.recv().await.expect("Should receive delete notification");
// 		assert_eq!(delete_change.subscription_id, sub_id);
//
// 		// Verify _op column indicates DELETE (3)
// 		let op = get_op_value(&delete_change.frame, 0);
// 		assert_eq!(op, Some(3), "_op should be 3 for DELETE");
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// fn test_op_multiple_types_in_sequence() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_op_multi", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert
// 		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;
// 		let insert_change = ctx.recv().await.expect("Should receive insert");
// 		assert_eq!(get_op_value(&insert_change.frame, 0), Some(1));
//
// 		// Update
// 		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;
// 		let update_change = ctx.recv().await.expect("Should receive update");
// 		assert_eq!(get_op_value(&update_change.frame, 0), Some(2));
//
// 		// Remove
// 		ctx.delete(&table, "id == 1").await?;
// 		let delete_change = ctx.recv().await.expect("Should receive delete");
// 		assert_eq!(get_op_value(&delete_change.frame, 0), Some(3));
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// fn test_op_batch_consecutive_rows() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_op_batch", "id: int4, name: utf8").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert 10 rows at once
// 		let rows: Vec<String> = (1..=10).map(|i| format!("{{ id: {}, name: 'user{}' }}", i, i)).collect();
// 		ctx.insert(&table, &rows.join(", ")).await?;
//
// 		let change = ctx.recv().await.expect("Should receive batch notification");
//
// 		// Should be batched into one notification with all 10 rows
// 		let id_col = find_column(&change.frame, "id").expect("id column should exist");
// 		assert_eq!(id_col.data.len(), 10, "Should have 10 rows");
//
// 		// Verify all 10 user rows
// 		let name_col = find_column(&change.frame, "name").expect("name column should exist");
// 		for i in 1..=10 {
// 			assert!(name_col.data.contains(&format!("user{}", i)), "Should contain user{}", i);
// 		}
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// // ============================================================================
// // Concurrent Subscriptions
// // ============================================================================
//
// #[test]
// fn test_concurrent_multiple_subscriptions() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table1 = unique_table_name("sub_conc_1");
// 		let table2 = unique_table_name("sub_conc_2");
//
// 		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
// 		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();
//
// 		let sub1 = client.subscribe(&format!("from test.{}", table1)).await.unwrap();
// 		let sub2 = client.subscribe(&format!("from test.{}", table2)).await.unwrap();
//
// 		// Insert into table 1
// 		client.command(&format!("INSERT test. [{{ id: 1, name: 'alice' }}]{}", table1), None).await.unwrap();
//
// 		let change1 = recv_with_timeout(&mut client, 5000).await.expect("Should receive change from table1");
// 		assert_eq!(change1.subscription_id, sub1);
//
// 		// Insert into table 2
// 		client.command(&format!("INSERT test. [{{ id: 2, value: 200 }}]{}", table2), None).await.unwrap();
//
// 		let change2 = recv_with_timeout(&mut client, 5000).await.expect("Should receive change from table2");
// 		assert_eq!(change2.subscription_id, sub2);
//
// 		client.unsubscribe(&sub1).await.unwrap();
// 		client.unsubscribe(&sub2).await.unwrap();
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_concurrent_5_plus_subscriptions() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		const NUM_TABLES: usize = 5;
// 		let mut tables = Vec::new();
// 		let mut sub_ids = Vec::new();
//
// 		// Create all tables and subscribe
// 		for i in 0..NUM_TABLES {
// 			let table = unique_table_name(&format!("sub_conc_{}", i));
// 			create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
// 			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			tables.push(table);
// 			sub_ids.push(sub_id);
// 		}
//
// 		// Insert into all tables
// 		for (i, table) in tables.iter().enumerate() {
// 			client
// 				.command(&format!("INSERT test. [{{ id: {}, value: {} }}]{}", i, i * 100, table), None)
// 				.await
// 				.unwrap();
// 		}
//
// 		// Wait for all callbacks
// 		let changes = recv_multiple_with_timeout(&mut client, NUM_TABLES, 15000).await;
// 		assert_eq!(changes.len(), NUM_TABLES, "Should receive {} notifications", NUM_TABLES);
//
// 		// Verify all subscription IDs are represented
// 		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
// 		for sub_id in &sub_ids {
// 			assert!(received_sub_ids.contains(sub_id.as_str()), "Missing notification for {}", sub_id);
// 		}
//
// 		// Cleanup subscriptions
// 		for sub_id in &sub_ids {
// 			client.unsubscribe(sub_id).await.unwrap();
// 		}
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// // ============================================================================
// // Reconnection Behavior
// // ============================================================================
//
// #[test]
// fn test_reconnection_resubscribe_after_disconnect() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table = unique_table_name("sub_reconn");
// 		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
//
// 		let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 		assert!(!sub_id.is_empty(), "Subscription ID should be defined");
//
// 		// Close and reconnect
// 		client.close().await.unwrap();
//
// 		// Reconnect
// 		let mut client2 = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client2.authenticate("mysecrettoken").await.unwrap();
//
// 		// Resubscribe
// 		let sub_id2 = client2.subscribe(&format!("from test.{}", table)).await.unwrap();
//
// 		// Insert new data
// 		client2.command(&format!("INSERT test. [{{ id: 1, name: 'after_reconnect' }}]{}", table),
// None).await.unwrap();
//
// 		let change = recv_with_timeout(&mut client2, 5000).await.expect("Should receive notification after reconnect");
// 		assert_eq!(change.subscription_id, sub_id2);
//
// 		let name_col = find_column(&change.frame, "name").expect("name column should exist");
// 		assert_eq!(name_col.data[0], "after_reconnect");
//
// 		client2.unsubscribe(&sub_id2).await.unwrap();
// 		client2.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_reconnection_multiple_subscriptions() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let tables: Vec<String> = (0..3).map(|i| unique_table_name(&format!("sub_reconn_m{}", i))).collect();
//
// 		for table in &tables {
// 			create_test_table(&client, table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
// 		}
//
// 		// Subscribe to all tables
// 		let mut sub_ids = Vec::new();
// 		for table in &tables {
// 			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			sub_ids.push(sub_id);
// 		}
//
// 		// Close and reconnect
// 		client.close().await.unwrap();
//
// 		let mut client2 = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client2.authenticate("mysecrettoken").await.unwrap();
//
// 		// Resubscribe to all tables
// 		let mut sub_ids2 = Vec::new();
// 		for table in &tables {
// 			let sub_id = client2.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			sub_ids2.push(sub_id);
// 		}
//
// 		// Insert into all tables
// 		for (i, table) in tables.iter().enumerate() {
// 			client2.command(&format!("INSERT test. [{{ id: {}, value: {} }}]{}", i, i * 100, table),
// None).await.unwrap(); 		}
//
// 		let changes = recv_multiple_with_timeout(&mut client2, 3, 10000).await;
// 		assert_eq!(changes.len(), 3, "Should receive 3 notifications");
//
// 		// Verify all subscription IDs are represented
// 		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
// 		for sub_id in &sub_ids2 {
// 			assert!(received_sub_ids.contains(sub_id.as_str()));
// 		}
//
// 		for sub_id in &sub_ids2 {
// 			client2.unsubscribe(sub_id).await.unwrap();
// 		}
// 		client2.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// // ============================================================================
// // Error Handling
// // ============================================================================
//
// #[test]
// fn test_error_invalid_query() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let result = client.subscribe("INVALID RQL SYNTAX HERE").await;
// 		assert!(result.is_err(), "Should reject subscription with invalid query");
//
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_error_nonexistent_table() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let non_existent_table = format!("table_that_does_not_exist_{}", std::time::SystemTime::now()
// 			.duration_since(std::time::UNIX_EPOCH)
// 			.unwrap()
// 			.as_millis());
//
// 		let result = client.subscribe(&format!("from {}", non_existent_table)).await;
// 		assert!(result.is_err(), "Should reject subscription to non-existent table");
//
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_error_invalid_subscription_id() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let fake_id = format!("fake-subscription-id-{}", std::time::SystemTime::now()
// 			.duration_since(std::time::UNIX_EPOCH)
// 			.unwrap()
// 			.as_millis());
//
// 		// May or may not throw depending on server implementation
// 		// Just verify it doesn't panic
// 		let _ = client.unsubscribe(&fake_id).await;
//
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// // ============================================================================
// // Cleanup and Lifecycle
// // ============================================================================
//
// #[test]
// fn test_lifecycle_cleanup_on_disconnect() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table = unique_table_name("sub_cleanup");
// 		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
//
// 		let _sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
//
// 		// Close without explicit unsubscribe - should not panic
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_lifecycle_no_callbacks_after_unsubscribe() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_no_cb", "id: int4, value: int4").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Unsubscribe immediately
// 		ctx.client.unsubscribe(&sub_id).await?;
//
// 		// Insert data
// 		ctx.insert(&table, "{ id: 1, value: 100 }").await?;
//
// 		// Small wait to verify no callback fires
// 		sleep(Duration::from_millis(100)).await;
//
// 		// Should NOT receive any change
// 		let change = recv_with_timeout(&mut ctx.client, 500).await;
// 		assert!(change.is_none(), "Should NOT receive callbacks after unsubscribe");
//
// 		ctx.client.close().await?;
// 		Ok(())
// 	});
// }
//
// // ============================================================================
// // Edge Cases
// // ============================================================================
//
// #[test]
// fn test_edge_empty_result_sets() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_empty", "id: int4, value: int4").await?;
//
// 		// Subscribe with filter that won't match
// 		let sub_id = ctx.client.subscribe(&format!("from test.{} filter {{ id > 1000 }}", table)).await?;
//
// 		// Insert data that doesn't match filter
// 		ctx.insert(&table, "{ id: 1, value: 100 }").await?;
//
// 		// Small wait to verify no callback fires for non-matching data
// 		sleep(Duration::from_millis(100)).await;
//
// 		let change = recv_with_timeout(&mut ctx.client, 500).await;
// 		assert!(change.is_none(), "Should not trigger callback for non-matching data");
//
// 		// Insert data that matches filter
// 		ctx.insert(&table, "{ id: 1001, value: 200 }").await?;
//
// 		let change = recv_with_timeout(&mut ctx.client, 5000).await.expect("Should receive matching data");
//
// 		// Verify matching row data
// 		let id_col = find_column(&change.frame, "id").expect("id column should exist");
// 		assert_eq!(id_col.data[0], "1001");
//
// 		let value_col = find_column(&change.frame, "value").expect("value column should exist");
// 		assert_eq!(value_col.data[0], "200");
//
// 		ctx.client.unsubscribe(&sub_id).await?;
// 		ctx.client.close().await?;
// 		Ok(())
// 	});
// }
//
// #[test]
// fn test_edge_large_batch_of_changes() {
// 	SubscriptionTestHarness::run(|mut ctx| async move {
// 		let table = ctx.create_table("sub_large", "id: int4, value: int4").await?;
// 		let sub_id = ctx.subscribe(&table).await?;
//
// 		// Insert 100 rows
// 		let rows: Vec<String> = (0..100).map(|i| format!("{{ id: {}, value: {} }}", i, i * 10)).collect();
// 		ctx.insert(&table, &rows.join(", ")).await?;
//
// 		let change = ctx.recv().await.expect("Should receive batch notification");
//
// 		// Should have received all 100 rows
// 		let id_col = find_column(&change.frame, "id").expect("id column should exist");
// 		assert_eq!(id_col.data.len(), 100, "Should have 100 rows");
//
// 		// Verify sample rows
// 		assert!(id_col.data.contains(&"0".to_string()));
// 		assert!(id_col.data.contains(&"49".to_string()));
// 		assert!(id_col.data.contains(&"99".to_string()));
//
// 		ctx.close(&sub_id).await
// 	});
// }
//
// #[test]
// #[ignore]
// fn test_edge_rapid_successive_changes() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table = unique_table_name("sub_rapid");
// 		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
//
// 		let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
//
// 		// Fire 10 insert commands rapidly
// 		for i in 0..10 {
// 			client
// 				.command(&format!("INSERT test. [{{ id: {}, value: {} }}]{}", i, i * 10, table), None)
// 				.await
// 				.unwrap();
// 		}
//
// 		// Collect all changes with timeout
// 		let changes = recv_multiple_with_timeout(&mut client, 10, 15000).await;
//
// 		// Count total rows received
// 		let total_rows: usize = changes.iter().map(|c| find_column(&c.frame, "id").map(|col|
// col.data.len()).unwrap_or(0)).sum(); 		assert_eq!(total_rows, 10, "Should have received all 10 rows");
//
// 		client.unsubscribe(&sub_id).await.unwrap();
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// // ============================================================================
// // Stress Tests - Connection Reliability
// // ============================================================================
//
// #[test]
// #[ignore]
// fn test_stress_many_subscriptions_single_client() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		const NUM_SUBS: usize = 50;
// 		let mut sub_ids = Vec::new();
// 		let mut tables = Vec::new();
//
// 		// Create 50 tables and subscriptions
// 		for i in 0..NUM_SUBS {
// 			let table = unique_table_name(&format!("stress_{}", i));
// 			create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
// 			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			sub_ids.push(sub_id);
// 			tables.push(table);
// 		}
//
// 		// Insert into all tables
// 		for table in &tables {
// 			client.command(&format!("INSERT test. [{{ id: 1 }}]{}", table), None).await.unwrap();
// 		}
//
// 		// Receive all notifications
// 		let changes = recv_multiple_with_timeout(&mut client, NUM_SUBS, 30000).await;
// 		assert_eq!(changes.len(), NUM_SUBS, "Should receive {} notifications", NUM_SUBS);
//
// 		// Verify all subscription IDs are represented
// 		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
// 		for sub_id in &sub_ids {
// 			assert!(received_sub_ids.contains(sub_id.as_str()), "Missing notification for {}", sub_id);
// 		}
//
// 		// Cleanup
// 		for sub_id in &sub_ids {
// 			client.unsubscribe(sub_id).await.unwrap();
// 		}
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_many_concurrent_clients() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		const NUM_CLIENTS: usize = 20;
//
// 		// Setup shared table
// 		let mut setup_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		setup_client.authenticate("mysecrettoken").await.unwrap();
//
// 		let shared_table = unique_table_name("stress_concurrent");
// 		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
// 		setup_client.close().await.unwrap();
//
// 		let received_count = Arc::new(AtomicUsize::new(0));
//
// 		// Spawn all clients concurrently
// 		let mut handles = Vec::new();
// 		for client_idx in 0..NUM_CLIENTS {
// 			let port = port;
// 			let table = shared_table.clone();
// 			let counter = Arc::clone(&received_count);
//
// 			let handle = tokio::spawn(async move {
// 				let mut client = WsClient::connect("127.0.0.1:8090").await?;
// 				client.authenticate("mysecrettoken").await?;
//
// 				let _sub_id = client.subscribe(&format!("from test.{}", table)).await?;
//
// 				let change = recv_with_timeout(&mut client, 10000).await;
// 				if change.is_some() {
// 					counter.fetch_add(1, Ordering::SeqCst);
// 				}
//
// 				client.close().await?;
// 				Ok::<_, Box<dyn Error + Send + Sync>>(())
// 			});
// 			handles.push((client_idx, handle));
// 		}
//
// 		// Give clients time to connect and subscribe
// 		sleep(Duration::from_millis(500)).await;
//
// 		// Trigger insert
// 		let mut trigger_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		trigger_client.authenticate("mysecrettoken").await.unwrap();
// 		trigger_client.command(&format!("INSERT test. [{{ id: 999 }}]{}", shared_table), None).await.unwrap();
// 		trigger_client.close().await.unwrap();
//
// 		// Wait for all clients
// 		for (idx, handle) in handles {
// 			match handle.await {
// 				Ok(Ok(())) => {}
// 				Ok(Err(e)) => eprintln!("Client {} failed: {}", idx, e),
// 				Err(e) => eprintln!("Client {} task panicked: {}", idx, e),
// 			}
// 		}
//
// 		let count = received_count.load(Ordering::SeqCst);
// 		assert_eq!(count, NUM_CLIENTS, "All {} clients should receive notification, got {}", NUM_CLIENTS, count);
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_rapid_subscribe_unsubscribe() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table = unique_table_name("stress_rapid");
// 		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
//
// 		const NUM_CYCLES: usize = 100;
// 		for i in 0..NUM_CYCLES {
// 			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			client.unsubscribe(&sub_id).await.unwrap();
//
// 			if (i + 1) % 25 == 0 {
// 				eprintln!("Completed {} rapid cycles", i + 1);
// 			}
// 		}
//
// 		// Verify system still works
// 		let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 		assert!(!sub_id.is_empty(), "Should get valid subscription after rapid cycles");
//
// 		client.command(&format!("INSERT test. [{{ id: 999 }}]{}", table), None).await.unwrap();
//
// 		let change = recv_with_timeout(&mut client, 5000).await;
// 		assert!(change.is_some(), "Should still receive changes after {} rapid cycles", NUM_CYCLES);
//
// 		client.unsubscribe(&sub_id).await.unwrap();
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_client_disconnect_without_unsubscribe() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		const NUM_CLIENTS: usize = 10;
//
// 		// Setup shared table
// 		let mut setup_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		setup_client.authenticate("mysecrettoken").await.unwrap();
//
// 		let shared_table = unique_table_name("stress_disconnect");
// 		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
// 		setup_client.close().await.unwrap();
//
// 		// Connect multiple clients and disconnect without unsubscribing
// 		for i in 0..NUM_CLIENTS {
// 			let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 			client.authenticate("mysecrettoken").await.unwrap();
// 			let _sub_id = client.subscribe(&format!("from test.{}", shared_table)).await.unwrap();
//
// 			// Drop without unsubscribe - simulates abrupt disconnect
// 			drop(client);
//
// 			if (i + 1) % 5 == 0 {
// 				eprintln!("Dropped {} clients abruptly", i + 1);
// 			}
// 		}
//
// 		// Give server time to clean up
// 		sleep(Duration::from_millis(500)).await;
//
// 		// Server should still be healthy
// 		let mut new_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		new_client.authenticate("mysecrettoken").await.unwrap();
//
// 		let sub_id = new_client.subscribe(&format!("from test.{}", shared_table)).await.unwrap();
// 		assert!(!sub_id.is_empty(), "New client should be able to subscribe after abrupt disconnects");
//
// 		new_client.command(&format!("INSERT test. [{{ id: 1 }}]{}", shared_table), None).await.unwrap();
//
// 		let change = recv_with_timeout(&mut new_client, 5000).await;
// 		assert!(change.is_some(), "New client should receive notification");
//
// 		new_client.unsubscribe(&sub_id).await.unwrap();
// 		new_client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_concurrent_connect_disconnect() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		const NUM_TASKS: usize = 10;
// 		const ITERATIONS_PER_TASK: usize = 5;
//
// 		// Setup tables
// 		let mut setup_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		setup_client.authenticate("mysecrettoken").await.unwrap();
//
// 		let mut tables = Vec::new();
// 		for i in 0..NUM_TASKS {
// 			let table = unique_table_name(&format!("stress_concurrent_{}", i));
// 			create_test_table(&setup_client, &table, &[("id", "int4")]).await.unwrap();
// 			tables.push(table);
// 		}
// 		setup_client.close().await.unwrap();
//
// 		let success_count = Arc::new(AtomicUsize::new(0));
//
// 		// Spawn concurrent tasks
// 		let mut handles = Vec::new();
// 		for task_idx in 0..NUM_TASKS {
// 			let port = port;
// 			let table = tables[task_idx].clone();
// 			let counter = Arc::clone(&success_count);
//
// 			let handle = tokio::spawn(async move {
// 				for iter in 0..ITERATIONS_PER_TASK {
// 					let mut retries = 0;
// 					const MAX_RETRIES: usize = 3;
//
// 					loop {
// 						let mut client = WsClient::connect("127.0.0.1:8090").await?;
// 						client.authenticate("mysecrettoken").await?;
//
// 						match client.subscribe(&format!("from test.{}", table)).await {
// 							Ok(sub_id) => {
// 								sleep(Duration::from_millis(10)).await;
// 								client.unsubscribe(&sub_id).await?;
// 								client.close().await?;
// 								counter.fetch_add(1, Ordering::SeqCst);
// 								break;
// 							}
// 							Err(e) if retries < MAX_RETRIES && e.to_string().contains("TXN_001") => {
// 								retries += 1;
// 								client.close().await?;
// 								sleep(Duration::from_millis(10 * retries as u64)).await;
// 								continue;
// 							}
// 							Err(e) => {
// 								client.close().await?;
// 								return Err(e.into());
// 							}
// 						}
// 					}
//
// 					if iter == ITERATIONS_PER_TASK - 1 {
// 						eprintln!("Task {} completed all {} iterations", task_idx, ITERATIONS_PER_TASK);
// 					}
// 				}
// 				Ok::<_, Box<dyn Error + Send + Sync>>(())
// 			});
// 			handles.push((task_idx, handle));
// 		}
//
// 		// Wait for all tasks
// 		for (idx, handle) in handles {
// 			match handle.await {
// 				Ok(Ok(())) => {}
// 				Ok(Err(e)) => eprintln!("Task {} failed: {}", idx, e),
// 				Err(e) => eprintln!("Task {} panicked: {}", idx, e),
// 			}
// 		}
//
// 		let count = success_count.load(Ordering::SeqCst);
// 		let expected = NUM_TASKS * ITERATIONS_PER_TASK;
// 		assert_eq!(count, expected, "All {} connect/disconnect cycles should succeed, got {}", expected, count);
//
// 		// Verify server is still healthy
// 		let mut final_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		final_client.authenticate("mysecrettoken").await.unwrap();
//
// 		let sub_id = final_client.subscribe(&format!("from test.{}", tables[0])).await.unwrap();
// 		assert!(!sub_id.is_empty(), "Server should still accept new subscriptions");
//
// 		final_client.command(&format!("INSERT test. [{{ id: 1 }}]{}", tables[0]), None).await.unwrap();
//
// 		let change = recv_with_timeout(&mut final_client, 5000).await;
// 		assert!(change.is_some(), "Server should still deliver notifications after stress test");
//
// 		final_client.unsubscribe(&sub_id).await.unwrap();
// 		final_client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// #[ignore]
// fn test_stress_subscribe_receive_unsubscribe_cycles() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		client.authenticate("mysecrettoken").await.unwrap();
//
// 		let table = unique_table_name("stress_full_cycle");
// 		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
//
// 		const NUM_CYCLES: usize = 200;
// 		for i in 0..NUM_CYCLES {
// 			let sub_id = client.subscribe(&format!("from test.{}", table)).await.unwrap();
// 			client.command(&format!("INSERT test. [{{ id: {} }}]{}", i, table), None).await.unwrap();
//
// 			let change = recv_with_timeout(&mut client, 500).await;
// 			assert!(change.is_some(), "Cycle {}: should receive notification", i);
//
// 			client.unsubscribe(&sub_id).await.unwrap();
//
// 			if (i + 1) % 50 == 0 {
// 				eprintln!("Completed {} full cycles", i + 1);
// 			}
// 		}
//
// 		client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_connection_churn() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		const NUM_CONNECTIONS: usize = 50;
//
// 		// Rapidly connect and disconnect without doing any operations
// 		for i in 0..NUM_CONNECTIONS {
// 			let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 			client.authenticate("mysecrettoken").await.unwrap();
// 			client.close().await.unwrap();
//
// 			if (i + 1) % 10 == 0 {
// 				eprintln!("Rapid connect/disconnect: {} completed", i + 1);
// 			}
// 		}
//
// 		// Verify server is still healthy
// 		let mut final_client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 		final_client.authenticate("mysecrettoken").await.unwrap();
//
// 		// Simple query to verify server is responsive
// 		let _ = final_client.command("create namespace stress_test_ns", None).await;
//
// 		final_client.close().await.unwrap();
// 	});
//
// 	cleanup_server(Some(server));
// }
//
// #[test]
// fn test_stress_connect_query_disconnect_cycles() {
// 	let runtime = Arc::new(Runtime::new().unwrap());
// 	let _guard = runtime.enter();
// 	let mut server = create_server_instance(&runtime);
// 	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();
//
// 	runtime.block_on(async {
// 		const NUM_CYCLES: usize = 30;
//
// 		for i in 0..NUM_CYCLES {
// 			let mut client = WsClient::connect("127.0.0.1:8090").await.unwrap();
// 			client.authenticate("mysecrettoken").await.unwrap();
//
// 			// Simple operation to verify connection works
// 			let _ = client.command("create namespace stress_test_ns", None).await;
//
// 			client.close().await.unwrap();
//
// 			if (i + 1) % 10 == 0 {
// 				eprintln!("Connect/query/disconnect: {} completed", i + 1);
// 			}
// 		}
// 	});
//
// 	cleanup_server(Some(server));
// }
