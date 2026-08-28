// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	error::Error,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::{SystemTime, UNIX_EPOCH},
};

use reifydb_client::{SubscriptionConfig, WireFormat, WsClient};
use reifydb_value::value::duration::Duration;
use tokio::{runtime::Runtime, time::sleep};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{
		SubscriptionTestHarness, create_test_table, find_column, get_op_value, get_row_numbers,
		recv_multiple_with_timeout, recv_with_timeout, unique_table_name,
	},
};

#[test]
fn test_basic_subscribe_to_query() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_basic", "id: int4, name: utf8, value: int4").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		assert!(!sub_id.is_empty(), "Subscription ID should be defined");
		assert!(sub_id.len() > 0, "Subscription ID should have length > 0");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_basic_unsubscribe_success() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_unsub", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		assert!(!sub_id.is_empty(), "Subscription ID should be defined");

		ctx.client.unsubscribe(&sub_id).await?;
		ctx.client.close().await?;
		Ok(())
	});
}

#[test]
fn test_basic_receive_insert_notifications() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_insert", "id: int4, name: utf8, value: int4").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'test', value: 100 }").await?;

		let change = ctx.recv().await.expect("Should receive insert notification");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload[0], "1");

		let name_col = find_column(&change.body, "name").expect("name column should exist");
		assert_eq!(name_col.payload[0], "test");

		let value_col = find_column(&change.body, "value").expect("value column should exist");
		assert_eq!(value_col.payload[0], "100");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_op_insert_callback() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_op_insert", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;

		let change = ctx.recv().await.expect("Should receive insert notification");
		assert_eq!(change.subscription_id, sub_id);

		let op = get_op_value(&change.body, 0);
		assert_eq!(op, Some(1), "op should be 1 for INSERT");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload.len(), 2, "Should have 2 rows");

		let name_col = find_column(&change.body, "name").expect("name column should exist");
		assert!(name_col.payload.contains(&"alice".to_string()));
		assert!(name_col.payload.contains(&"bob".to_string()));

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_op_update_callback() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_op_update", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_change = ctx.recv().await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_change.body, 0);
		assert_eq!(insert_op, Some(1), "op should be 1 for INSERT");

		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;

		let update_change = ctx.recv().await.expect("Should receive update notification");
		assert_eq!(update_change.subscription_id, sub_id);

		let op = get_op_value(&update_change.body, 0);
		assert_eq!(op, Some(2), "op should be 2 for UPDATE");

		let name_col = find_column(&update_change.body, "name").expect("name column should exist");
		assert_eq!(name_col.payload[0], "alice_updated");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_op_remove_callback() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_op_remove", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }, { id: 2, name: 'bob' }").await?;
		let insert_change = ctx.recv().await.expect("Should receive insert notification");
		let insert_op = get_op_value(&insert_change.body, 0);
		assert_eq!(insert_op, Some(1), "op should be 1 for INSERT");

		ctx.delete(&table, "id == 1").await?;

		let delete_change = ctx.recv().await.expect("Should receive delete notification");
		assert_eq!(delete_change.subscription_id, sub_id);

		let op = get_op_value(&delete_change.body, 0);
		assert_eq!(op, Some(3), "op should be 3 for DELETE");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_op_multiple_types_in_sequence() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_op_multi", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'alice' }").await?;
		let insert_change = ctx.recv().await.expect("Should receive insert");
		assert_eq!(get_op_value(&insert_change.body, 0), Some(1));

		ctx.update(&table, "id == 1", "id: id, name: 'alice_updated'").await?;
		let update_change = ctx.recv().await.expect("Should receive update");
		assert_eq!(get_op_value(&update_change.body, 0), Some(2));

		ctx.delete(&table, "id == 1").await?;
		let delete_change = ctx.recv().await.expect("Should receive delete");
		assert_eq!(get_op_value(&delete_change.body, 0), Some(3));

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_op_batch_consecutive_rows() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_op_batch", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		let rows: Vec<String> = (1..=10).map(|i| format!("{{ id: {}, name: 'user{}' }}", i, i)).collect();
		ctx.insert(&table, &rows.join(", ")).await?;

		// One command must arrive as one notification carrying all ten rows, not ten pushes.
		let change = ctx.recv().await.expect("Should receive batch notification");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload.len(), 10, "Should have 10 rows");

		let name_col = find_column(&change.body, "name").expect("name column should exist");
		for i in 1..=10 {
			assert!(name_col.payload.contains(&format!("user{}", i)), "Should contain user{}", i);
		}

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_concurrent_multiple_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table1 = unique_table_name("sub_conc_1");
		let table2 = unique_table_name("sub_conc_2");

		create_test_table(&client, &table1, &[("id", "int4"), ("name", "utf8")]).await.unwrap();
		create_test_table(&client, &table2, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let sub1 = client
			.subscribe(&format!("from test::{}", table1), SubscriptionConfig::default())
			.await
			.unwrap();
		let sub2 = client
			.subscribe(&format!("from test::{}", table2), SubscriptionConfig::default())
			.await
			.unwrap();

		client.command(&format!("INSERT test::{} [{{ id: 1, name: 'alice' }}]", table1), None).await.unwrap();

		let change1 = recv_with_timeout(&mut client, 5000).await.expect("Should receive change from table1");
		assert_eq!(change1.subscription_id, sub1);

		client.command(&format!("INSERT test::{} [{{ id: 2, value: 200 }}]", table2), None).await.unwrap();

		let change2 = recv_with_timeout(&mut client, 5000).await.expect("Should receive change from table2");
		assert_eq!(change2.subscription_id, sub2);

		client.unsubscribe(&sub1).await.unwrap();
		client.unsubscribe(&sub2).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_concurrent_5_plus_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		const NUM_TABLES: usize = 5;
		let mut tables = Vec::new();
		let mut sub_ids = Vec::new();

		for i in 0..NUM_TABLES {
			let table = unique_table_name(&format!("sub_conc_{}", i));
			create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			tables.push(table);
			sub_ids.push(sub_id);
		}

		for (i, table) in tables.iter().enumerate() {
			client.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 100), None)
				.await
				.unwrap();
		}

		let changes = recv_multiple_with_timeout(&mut client, NUM_TABLES, 15000).await;
		assert_eq!(changes.len(), NUM_TABLES, "Should receive {} notifications", NUM_TABLES);

		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
		for sub_id in &sub_ids {
			assert!(received_sub_ids.contains(sub_id.as_str()), "Missing notification for {}", sub_id);
		}

		for sub_id in &sub_ids {
			client.unsubscribe(sub_id).await.unwrap();
		}
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_reconnection_resubscribe_after_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_reconn");
		create_test_table(&client, &table, &[("id", "int4"), ("name", "utf8")]).await.unwrap();

		let sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();
		assert!(!sub_id.is_empty(), "Subscription ID should be defined");

		client.close().await.unwrap();

		let mut client2 = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client2.authenticate("mysecrettoken").await.unwrap();

		let sub_id2 = client2
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		client2.command(&format!("INSERT test::{} [{{ id: 1, name: 'after_reconnect' }}]", table), None)
			.await
			.unwrap();

		let change = recv_with_timeout(&mut client2, 5000)
			.await
			.expect("Should receive notification after reconnect");
		assert_eq!(change.subscription_id, sub_id2);

		let name_col = find_column(&change.body, "name").expect("name column should exist");
		assert_eq!(name_col.payload[0], "after_reconnect");

		client2.unsubscribe(&sub_id2).await.unwrap();
		client2.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_reconnection_multiple_subscriptions() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let tables: Vec<String> = (0..3).map(|i| unique_table_name(&format!("sub_reconn_m{}", i))).collect();

		for table in &tables {
			create_test_table(&client, table, &[("id", "int4"), ("value", "int4")]).await.unwrap();
		}

		let mut sub_ids = Vec::new();
		for table in &tables {
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			sub_ids.push(sub_id);
		}

		client.close().await.unwrap();

		let mut client2 = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client2.authenticate("mysecrettoken").await.unwrap();

		let mut sub_ids2 = Vec::new();
		for table in &tables {
			let sub_id = client2
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			sub_ids2.push(sub_id);
		}

		for (i, table) in tables.iter().enumerate() {
			client2.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 100), None)
				.await
				.unwrap();
		}

		let changes = recv_multiple_with_timeout(&mut client2, 3, 10000).await;
		assert_eq!(changes.len(), 3, "Should receive 3 notifications");

		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
		for sub_id in &sub_ids2 {
			assert!(received_sub_ids.contains(sub_id.as_str()));
		}

		for sub_id in &sub_ids2 {
			client2.unsubscribe(sub_id).await.unwrap();
		}
		client2.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_error_invalid_query() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let result = client.subscribe("INVALID RQL SYNTAX HERE", SubscriptionConfig::default()).await;
		assert!(result.is_err(), "Should reject subscription with invalid query");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_error_nonexistent_table() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let non_existent_table = format!(
			"table_that_does_not_exist_{}",
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
		);

		let result =
			client.subscribe(&format!("from {}", non_existent_table), SubscriptionConfig::default()).await;
		assert!(result.is_err(), "Should reject subscription to non-existent table");

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_error_invalid_subscription_id() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let fake_id = format!(
			"fake-subscription-id-{}",
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
		);

		// An unknown id may or may not error server-side; this only pins that it never panics.
		let _ = client.unsubscribe(&fake_id).await;

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_lifecycle_cleanup_on_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_cleanup");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		let _sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_lifecycle_no_callbacks_after_unsubscribe() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_no_cb", "id: int4, value: int4").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.client.unsubscribe(&sub_id).await?;

		ctx.insert(&table, "{ id: 1, value: 100 }").await?;

		sleep(Duration::from_milliseconds(100).unwrap().to_std()).await;

		let change = recv_with_timeout(&mut ctx.client, 500).await;
		assert!(change.is_none(), "Should NOT receive callbacks after unsubscribe");

		ctx.client.close().await?;
		Ok(())
	});
}

#[test]
fn test_edge_empty_result_sets() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_empty", "id: int4, value: int4").await?;

		let sub_id = ctx
			.client
			.subscribe(
				&format!("from test::{} filter {{ id > 1000 }}", table),
				SubscriptionConfig::default(),
			)
			.await?;

		ctx.insert(&table, "{ id: 1, value: 100 }").await?;

		sleep(Duration::from_milliseconds(100).unwrap().to_std()).await;

		let change = recv_with_timeout(&mut ctx.client, 500).await;
		assert!(change.is_none(), "Should not trigger callback for non-matching data");

		ctx.insert(&table, "{ id: 1001, value: 200 }").await?;

		let change = recv_with_timeout(&mut ctx.client, 5000).await.expect("Should receive matching data");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload[0], "1001");

		let value_col = find_column(&change.body, "value").expect("value column should exist");
		assert_eq!(value_col.payload[0], "200");

		ctx.client.unsubscribe(&sub_id).await?;
		ctx.client.close().await?;
		Ok(())
	});
}

#[test]
fn test_edge_large_batch_of_changes() {
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_large", "id: int4, value: int4").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		let rows: Vec<String> = (0..100).map(|i| format!("{{ id: {}, value: {} }}", i, i * 10)).collect();
		ctx.insert(&table, &rows.join(", ")).await?;

		let change = ctx.recv().await.expect("Should receive batch notification");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload.len(), 100, "Should have 100 rows");

		assert!(id_col.payload.contains(&"0".to_string()));
		assert!(id_col.payload.contains(&"49".to_string()));
		assert!(id_col.payload.contains(&"99".to_string()));

		ctx.close(&sub_id).await
	});
}

#[test]
#[ignore]
fn test_edge_rapid_successive_changes() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("sub_rapid");
		create_test_table(&client, &table, &[("id", "int4"), ("value", "int4")]).await.unwrap();

		let sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();

		for i in 0..10 {
			client.command(&format!("INSERT test::{} [{{ id: {}, value: {} }}]", table, i, i * 10), None)
				.await
				.unwrap();
		}

		let changes = recv_multiple_with_timeout(&mut client, 10, 15000).await;

		// Rapid inserts may coalesce, so the invariant is total rows, not notification count.
		let total_rows: usize = changes
			.iter()
			.map(|c| find_column(&c.body, "id").map(|col| col.payload.len()).unwrap_or(0))
			.sum();
		assert_eq!(total_rows, 10, "Should have received all 10 rows");

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
#[ignore]
fn test_stress_many_subscriptions_single_client() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		const NUM_SUBS: usize = 50;
		let mut sub_ids = Vec::new();
		let mut tables = Vec::new();

		for i in 0..NUM_SUBS {
			let table = unique_table_name(&format!("stress_{}", i));
			create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			sub_ids.push(sub_id);
			tables.push(table);
		}

		for table in &tables {
			client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		}

		let changes = recv_multiple_with_timeout(&mut client, NUM_SUBS, 30000).await;
		assert_eq!(changes.len(), NUM_SUBS, "Should receive {} notifications", NUM_SUBS);

		let received_sub_ids: HashSet<_> = changes.iter().map(|c| c.subscription_id.as_str()).collect();
		for sub_id in &sub_ids {
			assert!(received_sub_ids.contains(sub_id.as_str()), "Missing notification for {}", sub_id);
		}

		for sub_id in &sub_ids {
			client.unsubscribe(sub_id).await.unwrap();
		}
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_many_concurrent_clients() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 20;

		let mut setup_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		setup_client.authenticate("mysecrettoken").await.unwrap();

		let shared_table = unique_table_name("stress_concurrent");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		setup_client.close().await.unwrap();

		let received_count = Arc::new(AtomicUsize::new(0));

		let mut handles = Vec::new();
		for client_idx in 0..NUM_CLIENTS {
			let port = port;
			let table = shared_table.clone();
			let counter = Arc::clone(&received_count);

			let handle = tokio::spawn(async move {
				let mut client =
					WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await?;
				client.authenticate("mysecrettoken").await?;

				let _sub_id = client
					.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
					.await?;

				let change = recv_with_timeout(&mut client, 10000).await;
				if change.is_some() {
					counter.fetch_add(1, Ordering::SeqCst);
				}

				client.close().await?;
				Ok::<_, Box<dyn Error + Send + Sync>>(())
			});
			handles.push((client_idx, handle));
		}

		sleep(Duration::from_milliseconds(500).unwrap().to_std()).await;

		let mut trigger_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		trigger_client.authenticate("mysecrettoken").await.unwrap();
		trigger_client.command(&format!("INSERT test::{} [{{ id: 999 }}]", shared_table), None).await.unwrap();
		trigger_client.close().await.unwrap();

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
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("stress_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		const NUM_CYCLES: usize = 100;
		for i in 0..NUM_CYCLES {
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			client.unsubscribe(&sub_id).await.unwrap();

			if (i + 1) % 25 == 0 {
				eprintln!("Completed {} rapid cycles", i + 1);
			}
		}

		let sub_id = client
			.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
			.await
			.unwrap();
		assert!(!sub_id.is_empty(), "Should get valid subscription after rapid cycles");

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let change = recv_with_timeout(&mut client, 5000).await;
		assert!(change.is_some(), "Should still receive changes after {} rapid cycles", NUM_CYCLES);

		client.unsubscribe(&sub_id).await.unwrap();
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_client_disconnect_without_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 10;

		let mut setup_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		setup_client.authenticate("mysecrettoken").await.unwrap();

		let shared_table = unique_table_name("stress_disconnect");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		setup_client.close().await.unwrap();

		for i in 0..NUM_CLIENTS {
			let mut client =
				WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
			client.authenticate("mysecrettoken").await.unwrap();
			let _sub_id = client
				.subscribe(&format!("from test::{}", shared_table), SubscriptionConfig::default())
				.await
				.unwrap();

			// Dropping without unsubscribe simulates an abrupt disconnect.
			drop(client);

			if (i + 1) % 5 == 0 {
				eprintln!("Dropped {} clients abruptly", i + 1);
			}
		}

		// Give server time to clean up
		sleep(Duration::from_milliseconds(500).unwrap().to_std()).await;

		let mut new_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		new_client.authenticate("mysecrettoken").await.unwrap();

		let sub_id = new_client
			.subscribe(&format!("from test::{}", shared_table), SubscriptionConfig::default())
			.await
			.unwrap();
		assert!(!sub_id.is_empty(), "New client should be able to subscribe after abrupt disconnects");

		new_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", shared_table), None).await.unwrap();

		let change = recv_with_timeout(&mut new_client, 5000).await;
		assert!(change.is_some(), "New client should receive notification");

		new_client.unsubscribe(&sub_id).await.unwrap();
		new_client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_concurrent_connect_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_TASKS: usize = 10;
		const ITERATIONS_PER_TASK: usize = 5;

		let mut setup_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		setup_client.authenticate("mysecrettoken").await.unwrap();

		let mut tables = Vec::new();
		for i in 0..NUM_TASKS {
			let table = unique_table_name(&format!("stress_concurrent_{}", i));
			create_test_table(&setup_client, &table, &[("id", "int4")]).await.unwrap();
			tables.push(table);
		}
		setup_client.close().await.unwrap();

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
						let mut client = WsClient::connect(
							&format!("ws://[::1]:{}", port),
							WireFormat::Frames,
						)
						.await?;
						client.authenticate("mysecrettoken").await?;

						match client
							.subscribe(
								&format!("from test::{}", table),
								SubscriptionConfig::default(),
							)
							.await
						{
							Ok(sub_id) => {
								sleep(Duration::from_milliseconds(10)
									.unwrap()
									.to_std())
								.await;
								client.unsubscribe(&sub_id).await?;
								client.close().await?;
								counter.fetch_add(1, Ordering::SeqCst);
								break;
							}
							Err(e) if retries < MAX_RETRIES
								&& e.to_string().contains("TXN_001") =>
							{
								retries += 1;
								client.close().await?;
								sleep(Duration::from_milliseconds(10 * retries as i64)
									.unwrap()
									.to_std())
								.await;
								continue;
							}
							Err(e) => {
								client.close().await?;
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
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		final_client.authenticate("mysecrettoken").await.unwrap();

		let sub_id = final_client
			.subscribe(&format!("from test::{}", tables[0]), SubscriptionConfig::default())
			.await
			.unwrap();
		assert!(!sub_id.is_empty(), "Server should still accept new subscriptions");

		final_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", tables[0]), None).await.unwrap();

		let change = recv_with_timeout(&mut final_client, 5000).await;
		assert!(change.is_some(), "Server should still deliver notifications after stress test");

		final_client.unsubscribe(&sub_id).await.unwrap();
		final_client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
#[ignore]
fn test_stress_subscribe_receive_unsubscribe_cycles() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		client.authenticate("mysecrettoken").await.unwrap();

		let table = unique_table_name("stress_full_cycle");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		const NUM_CYCLES: usize = 200;
		for i in 0..NUM_CYCLES {
			let sub_id = client
				.subscribe(&format!("from test::{}", table), SubscriptionConfig::default())
				.await
				.unwrap();
			client.command(&format!("INSERT test::{} [{{ id: {} }}]", table, i), None).await.unwrap();

			let change = recv_with_timeout(&mut client, 500).await;
			assert!(change.is_some(), "Cycle {}: should receive notification", i);

			client.unsubscribe(&sub_id).await.unwrap();

			if (i + 1) % 50 == 0 {
				eprintln!("Completed {} full cycles", i + 1);
			}
		}

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_connection_churn() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CONNECTIONS: usize = 50;

		for i in 0..NUM_CONNECTIONS {
			let mut client =
				WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
			client.authenticate("mysecrettoken").await.unwrap();
			client.close().await.unwrap();

			if (i + 1) % 10 == 0 {
				eprintln!("Rapid connect/disconnect: {} completed", i + 1);
			}
		}

		let mut final_client =
			WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
		final_client.authenticate("mysecrettoken").await.unwrap();

		let _ = final_client.command("create namespace stress_test_ns", None).await;

		final_client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_stress_connect_query_disconnect_cycles() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CYCLES: usize = 30;

		for i in 0..NUM_CYCLES {
			let mut client =
				WsClient::connect(&format!("ws://[::1]:{}", port), WireFormat::Frames).await.unwrap();
			client.authenticate("mysecrettoken").await.unwrap();

			let _ = client.command("create namespace stress_test_ns", None).await;

			client.close().await.unwrap();

			if (i + 1) % 10 == 0 {
				eprintln!("Connect/query/disconnect: {} completed", i + 1);
			}
		}
	});

	cleanup_server(Some(server));
}

#[test]
fn test_row_number_is_stable_across_insert_update_remove() {
	// A subscriber keys its local state on the row number, so the identity it inserts under must
	// be the identity it later updates and finally removes; if it drifted, the client would leak
	// the stale row and delete one it never saw.
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_rownum_lifecycle", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 1, name: 'a' }").await?;
		let insert = ctx.recv().await.expect("insert change");
		let inserted = get_row_numbers(&insert.body);
		assert_eq!(inserted.len(), 1, "the insert must carry exactly one row number");
		assert_eq!(get_op_value(&insert.body, 0), Some(1));

		ctx.update(&table, "id == 1", "name: 'b'").await?;
		let update = ctx.recv().await.expect("update change");
		assert_eq!(get_op_value(&update.body, 0), Some(2));
		assert_eq!(get_row_numbers(&update.body), inserted, "an update must not mint a new identity");

		ctx.delete(&table, "id == 1").await?;
		let remove = ctx.recv().await.expect("remove change");
		assert_eq!(get_op_value(&remove.body, 0), Some(3));
		assert_eq!(get_row_numbers(&remove.body), inserted, "a remove must name the row that was inserted");

		ctx.close(&sub_id).await
	});
}

#[test]
fn test_user_id_column_is_independent_of_the_row_number() {
	// `id` is an ordinary user column: it may repeat and it carries no identity. Keying on it
	// would make two rows sharing an `id` indistinguishable, which is exactly what the row number
	// exists to prevent.
	SubscriptionTestHarness::run(|mut ctx| async move {
		let table = ctx.create_table("sub_rownum_vs_id", "id: int4, name: utf8").await?;
		let sub_id = ctx.subscribe(&table, SubscriptionConfig::default()).await?;

		ctx.insert(&table, "{ id: 5, name: 'a' }, { id: 5, name: 'b' }").await?;
		let change = ctx.recv().await.expect("insert change");

		let id_col = find_column(&change.body, "id").expect("id column should exist");
		assert_eq!(id_col.payload, vec!["5".to_string(), "5".to_string()], "both rows share one user id");

		let row_numbers = get_row_numbers(&change.body);
		assert_eq!(row_numbers.len(), 2, "every row must carry its own row number");
		assert_ne!(row_numbers[0], row_numbers[1], "two rows sharing a user id must still be distinguishable");

		assert!(
			!id_col.payload.iter().any(|v| row_numbers.iter().any(|rn| rn.to_string() == *v)),
			"the row number must not be sourced from the user id column"
		);

		ctx.close(&sub_id).await
	});
}
