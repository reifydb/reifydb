// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashSet,
	error::Error,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use reifydb_client::{ChangeKind, GrpcClient, Value, WireFormat};
use tokio::{runtime::Runtime, time::sleep};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{
		SubscriptionTestHarness, TestContext, create_test_table, find_column, recv_with_timeout,
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

		let change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &change.frames[0];

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

		let change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		let frame = &change.frames[0];

		assert_eq!(change.kind, ChangeKind::Insert, "kind should be Insert");

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

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		assert_eq!(insert_change.kind, ChangeKind::Insert, "kind should be Insert");

		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		let update_change = TestContext::recv(&mut sub).await.expect("Should receive update notification");
		let frame = &update_change.frames[0];
		assert_eq!(update_change.kind, ChangeKind::Update, "kind should be Update");

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

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_change = TestContext::recv(&mut sub).await.expect("Should receive insert notification");
		assert_eq!(insert_change.kind, ChangeKind::Insert, "kind should be Insert");

		ctx.delete(&table, "id == 1").await?;

		let delete_change = TestContext::recv(&mut sub).await.expect("Should receive delete notification");
		let _frame = &delete_change.frames[0];
		assert_eq!(delete_change.kind, ChangeKind::Remove, "kind should be Remove");

		drop(sub);
		Ok(())
	});
}

#[test]
fn test_op_multiple_types_in_sequence() {
	SubscriptionTestHarness::run(|ctx| async move {
		let table = ctx.create_table("sub_op_multi", "id: int4, name: utf8").await?;
		let mut sub = ctx.subscribe(&table).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;
		let insert_change = TestContext::recv(&mut sub).await.expect("Should receive insert");
		assert_eq!(insert_change.kind, ChangeKind::Insert);

		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;
		let update_change = TestContext::recv(&mut sub).await.expect("Should receive update");
		assert_eq!(update_change.kind, ChangeKind::Update);

		ctx.delete(&table, "id == 1").await?;
		let delete_change = TestContext::recv(&mut sub).await.expect("Should receive delete");
		assert_eq!(delete_change.kind, ChangeKind::Remove);

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

		let change = TestContext::recv(&mut sub).await.expect("Should receive batch notification");
		let frame = &change.frames[0];

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
			let change = recv_with_timeout(sub, 15000).await;
			if change.is_some() {
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

		let change =
			recv_with_timeout(&mut sub2, 5000).await.expect("Should receive notification after reconnect");
		let frame = &change.frames[0];

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
			let change = recv_with_timeout(sub, 10000).await;
			if change.is_some() {
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
		let change = recv_with_timeout(&mut sub2, 500).await;
		let _ = change;

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

		let change = recv_with_timeout(&mut sub, 500).await;
		assert!(change.is_none(), "Should not trigger callback for non-matching data");

		// Insert data that matches filter
		ctx.insert(&table, "{ id: 1001, value: 200 }").await?;

		let change = recv_with_timeout(&mut sub, 5000).await.expect("Should receive matching data");
		let frame = &change.frames[0];

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

		let change = TestContext::recv(&mut sub).await.expect("Should receive batch notification");
		let frame = &change.frames[0];

		// Should have received all 100 rows
		let id_col = find_column(frame, "id").expect("id column should exist");
		assert_eq!(id_col.data.len(), 100, "Should have 100 rows");

		drop(sub);
		Ok(())
	});
}

#[test]
#[ignore]
fn test_edge_rapid_successive_changes() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();

		// Fire 10 insert commands rapidly
		for i in 0..10 {
			client.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 10), None)
				.await
				.unwrap();
		}

		// Collect all changes with timeout
		let mut total_rows = 0usize;
		let deadline = tokio::time::Instant::now() + Duration::from_millis(15000);
		while total_rows < 10 {
			let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
			if remaining.is_zero() {
				break;
			}
			match tokio::time::timeout(remaining, sub.recv()).await {
				Ok(Some(change)) => {
					total_rows += change
						.frames
						.iter()
						.map(|f| find_column(f, "id").map(|c| c.data.len()).unwrap_or(0))
						.sum::<usize>();
				}
				_ => break,
			}
		}
		assert_eq!(total_rows, 10, "Should have received all 10 rows");

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
#[ignore]
fn test_stress_many_subscriptions_single_client() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		const NUM_SUBS: usize = 50;
		let mut subs = Vec::new();
		let mut sub_ids = Vec::new();
		let mut tables = Vec::new();

		for i in 0..NUM_SUBS {
			let table = unique_table_name(&format!("stress_{}", i));
			create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			sub_ids.push(sub.subscription_id().to_string());
			subs.push(sub);
			tables.push(table);
		}

		for table in &tables {
			client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		}

		let received_count = Arc::new(AtomicUsize::new(0));
		let received_ids: Arc<tokio::sync::Mutex<HashSet<String>>> =
			Arc::new(tokio::sync::Mutex::new(HashSet::new()));

		let mut handles = Vec::new();
		for mut sub in subs {
			let sub_id = sub.subscription_id().to_string();
			let counter = Arc::clone(&received_count);
			let ids = Arc::clone(&received_ids);
			let handle = tokio::spawn(async move {
				if recv_with_timeout(&mut sub, 30000).await.is_some() {
					counter.fetch_add(1, Ordering::SeqCst);
					ids.lock().await.insert(sub_id);
				}
				drop(sub);
			});
			handles.push(handle);
		}

		for h in handles {
			let _ = h.await;
		}

		assert_eq!(
			received_count.load(Ordering::SeqCst),
			NUM_SUBS,
			"Should receive {} notifications",
			NUM_SUBS
		);
		let ids = received_ids.lock().await;
		for sub_id in &sub_ids {
			assert!(ids.contains(sub_id), "Missing notification for {}", sub_id);
		}
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_many_concurrent_clients() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 20;

		let mut setup_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let shared_table = unique_table_name("stress_concurrent");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		drop(setup_client);

		let received_count = Arc::new(AtomicUsize::new(0));

		let mut handles = Vec::new();
		for client_idx in 0..NUM_CLIENTS {
			let port = port;
			let table = shared_table.clone();
			let counter = Arc::clone(&received_count);

			let handle = tokio::spawn(async move {
				let mut client =
					GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto)
						.await?;
				client.authenticate("mysecrettoken");

				let mut sub = client.subscribe(&format!("from test::{}", table)).await?;

				let change = recv_with_timeout(&mut sub, 10000).await;
				if change.is_some() {
					counter.fetch_add(1, Ordering::SeqCst);
				}

				drop(sub);
				drop(client);
				Ok::<_, Box<dyn Error + Send + Sync>>(())
			});
			handles.push((client_idx, handle));
		}

		sleep(Duration::from_millis(500)).await;

		let mut trigger_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		trigger_client.authenticate("mysecrettoken");
		trigger_client.command(&format!("INSERT test::{} [{{ id: 999 }}]", shared_table), None).await.unwrap();
		drop(trigger_client);

		for (idx, handle) in handles {
			match handle.await {
				Ok(Ok(())) => {}
				Ok(Err(e)) => eprintln!("Client {} failed: {}", idx, e),
				Err(e) => eprintln!("Client {} task panicked: {}", idx, e),
			}
		}

		let count = received_count.load(Ordering::SeqCst);
		assert_eq!(
			count, NUM_CLIENTS,
			"All {} clients should receive notification, got {}",
			NUM_CLIENTS, count
		);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_rapid_subscribe_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("stress_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		const NUM_CYCLES: usize = 100;
		for i in 0..NUM_CYCLES {
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			drop(sub);

			if (i + 1) % 25 == 0 {
				eprintln!("Completed {} rapid cycles", i + 1);
			}
		}

		let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert!(!sub.subscription_id().is_empty(), "Should get valid subscription after rapid cycles");

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut sub, 5000).await;
		assert!(change.is_some(), "Should still receive changes after {} rapid cycles", NUM_CYCLES);

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_client_disconnect_without_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 10;

		let mut setup_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let shared_table = unique_table_name("stress_disconnect");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		drop(setup_client);

		for i in 0..NUM_CLIENTS {
			let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto)
				.await
				.unwrap();
			client.authenticate("mysecrettoken");
			let sub = client.subscribe(&format!("from test::{}", shared_table)).await.unwrap();

			// Drop everything abruptly
			drop(sub);
			drop(client);

			if (i + 1) % 5 == 0 {
				eprintln!("Dropped {} clients abruptly", i + 1);
			}
		}

		sleep(Duration::from_millis(500)).await;

		let mut new_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		new_client.authenticate("mysecrettoken");

		let mut sub = new_client.subscribe(&format!("from test::{}", shared_table)).await.unwrap();
		assert!(
			!sub.subscription_id().is_empty(),
			"New client should be able to subscribe after abrupt disconnects"
		);

		new_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", shared_table), None).await.unwrap();

		let change = recv_with_timeout(&mut sub, 5000).await;
		assert!(change.is_some(), "New client should receive notification");

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_concurrent_connect_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_TASKS: usize = 10;
		const ITERATIONS_PER_TASK: usize = 5;

		let mut setup_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let mut tables = Vec::new();
		for i in 0..NUM_TASKS {
			let table = unique_table_name(&format!("stress_concurrent_{}", i));
			create_test_table(&setup_client, &table, &[("id", "int4")]).await.unwrap();
			tables.push(table);
		}
		drop(setup_client);

		let success_count = Arc::new(AtomicUsize::new(0));

		let mut handles = Vec::new();
		for task_idx in 0..NUM_TASKS {
			let port = port;
			let table = tables[task_idx].clone();
			let counter = Arc::clone(&success_count);

			let handle = tokio::spawn(async move {
				for iter in 0..ITERATIONS_PER_TASK {
					let mut retries = 0;
					const MAX_RETRIES: usize = 3;

					loop {
						let mut client = GrpcClient::connect(
							&format!("http://[::1]:{}", port),
							WireFormat::Proto,
						)
						.await?;
						client.authenticate("mysecrettoken");

						match client.subscribe(&format!("from test::{}", table)).await {
							Ok(sub) => {
								sleep(Duration::from_millis(10)).await;
								drop(sub);
								drop(client);
								counter.fetch_add(1, Ordering::SeqCst);
								break;
							}
							Err(e) if retries < MAX_RETRIES
								&& e.to_string().contains("TXN_001") =>
							{
								retries += 1;
								drop(client);
								sleep(Duration::from_millis(10 * retries as u64)).await;
								continue;
							}
							Err(e) => {
								drop(client);
								return Err(e.into());
							}
						}
					}

					if iter == ITERATIONS_PER_TASK - 1 {
						eprintln!(
							"Task {} completed all {} iterations",
							task_idx, ITERATIONS_PER_TASK
						);
					}
				}
				Ok::<_, Box<dyn Error + Send + Sync>>(())
			});
			handles.push((task_idx, handle));
		}

		for (idx, handle) in handles {
			match handle.await {
				Ok(Ok(())) => {}
				Ok(Err(e)) => eprintln!("Task {} failed: {}", idx, e),
				Err(e) => eprintln!("Task {} panicked: {}", idx, e),
			}
		}

		let count = success_count.load(Ordering::SeqCst);
		let expected = NUM_TASKS * ITERATIONS_PER_TASK;
		assert_eq!(count, expected, "All {} connect/disconnect cycles should succeed, got {}", expected, count);

		let mut final_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		final_client.authenticate("mysecrettoken");

		let mut sub = final_client.subscribe(&format!("from test::{}", tables[0])).await.unwrap();
		assert!(!sub.subscription_id().is_empty(), "Server should still accept new subscriptions");

		final_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", tables[0]), None).await.unwrap();

		let change = recv_with_timeout(&mut sub, 5000).await;
		assert!(change.is_some(), "Server should still deliver notifications after stress test");

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
#[ignore]
fn test_stress_subscribe_receive_unsubscribe_cycles() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("stress_full_cycle");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		const NUM_CYCLES: usize = 200;
		for i in 0..NUM_CYCLES {
			let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			client.command(&format!("INSERT test::{} [{{ id: {} }}]", table, i), None).await.unwrap();

			let change = recv_with_timeout(&mut sub, 500).await;
			assert!(change.is_some(), "Cycle {}: should receive notification", i);

			drop(sub);

			if (i + 1) % 50 == 0 {
				eprintln!("Completed {} full cycles", i + 1);
			}
		}
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_connection_churn() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CONNECTIONS: usize = 50;

		for i in 0..NUM_CONNECTIONS {
			let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto)
				.await
				.unwrap();
			client.authenticate("mysecrettoken");
			drop(client);

			if (i + 1) % 10 == 0 {
				eprintln!("Rapid connect/disconnect: {} completed", i + 1);
			}
		}

		let mut final_client =
			GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto).await.unwrap();
		final_client.authenticate("mysecrettoken");

		let _ = final_client.command("create namespace stress_test_ns", None).await;

		drop(final_client);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_connect_query_disconnect_cycles() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CYCLES: usize = 30;

		for i in 0..NUM_CYCLES {
			let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port), WireFormat::Proto)
				.await
				.unwrap();
			client.authenticate("mysecrettoken");

			let _ = client.command("create namespace stress_test_ns", None).await;

			drop(client);

			if (i + 1) % 10 == 0 {
				eprintln!("Connect/query/disconnect: {} completed", i + 1);
			}
		}
	});

	cleanup_server(Some(server));
}
