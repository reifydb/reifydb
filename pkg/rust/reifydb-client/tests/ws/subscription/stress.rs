// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

use reifydb_client::{SubscriptionConfig, WireFormat, WsClient};
use reifydb_value::value::duration::Duration;
use tokio::{
	runtime::Runtime,
	time::{sleep, timeout},
};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_ws_port},
	ws::subscription::{create_test_table, recv_multiple_with_timeout, recv_with_timeout, unique_table_name},
};

#[test]
fn test_many_subscriptions_single_client() {
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
		assert_eq!(changes.len(), NUM_SUBS, "Should receive {} notifications, got {}", NUM_SUBS, changes.len());

		let received_sub_ids: std::collections::HashSet<_> =
			changes.iter().map(|c| c.subscription_id.as_str()).collect();
		for sub_id in &sub_ids {
			assert!(
				received_sub_ids.contains(sub_id.as_str()),
				"Missing notification for subscription {}",
				sub_id
			);
		}

		for sub_id in &sub_ids {
			client.unsubscribe(sub_id).await.unwrap();
		}
		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}

#[test]
fn test_many_concurrent_clients() {
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
fn test_rapid_subscribe_unsubscribe() {
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
fn test_client_disconnect_without_unsubscribe() {
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

			// Dropping without unsubscribe or close simulates an abrupt disconnect.
			drop(client);

			if (i + 1) % 5 == 0 {
				eprintln!("Dropped {} clients abruptly", i + 1);
			}
		}

		// Give server time to clean up disconnected clients
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
fn test_concurrent_connect_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_ws_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_TASKS: usize = 10;
		const ITERATIONS_PER_TASK: usize = 5;

		// Create a table for each task to avoid transaction conflicts
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
					// Retry loop for handling transient transaction conflicts
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
								break; // Success, exit retry loop
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
fn test_subscribe_receive_unsubscribe_cycles() {
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
		// A healthy op completes in milliseconds even under heavy parallel load, so 30s only trips
		// on a genuine hang, failing fast and attributed instead of stalling the run.
		let op_timeout = Duration::from_seconds(30).unwrap();
		for i in 0..NUM_CYCLES {
			let sub_id = timeout(
				op_timeout.to_std(),
				client.subscribe(&format!("from test::{}", table), SubscriptionConfig::default()),
			)
			.await
			.unwrap_or_else(|_| panic!("Cycle {}: subscribe timed out", i))
			.unwrap();
			timeout(
				op_timeout.to_std(),
				client.command(&format!("INSERT test::{} [{{ id: {} }}]", table, i), None),
			)
			.await
			.unwrap_or_else(|_| panic!("Cycle {}: command timed out", i))
			.unwrap();

			let change = recv_with_timeout(&mut client, 500).await;
			assert!(change.is_some(), "Cycle {}: should receive notification", i);

			timeout(op_timeout.to_std(), client.unsubscribe(&sub_id))
				.await
				.unwrap_or_else(|_| panic!("Cycle {}: unsubscribe timed out", i))
				.unwrap();
		}

		client.close().await.unwrap();
	});

	cleanup_server(Some(server));
}
