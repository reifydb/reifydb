// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	error::Error,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use reifydb_client::GrpcClient;
use tokio::{runtime::Runtime, time::sleep};

use crate::{
	common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port},
	grpc::subscription::{create_test_table, recv_with_timeout, unique_table_name},
};

/// Test creating many subscriptions from a single client
#[test]
fn test_many_subscriptions_single_client() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		// Create 50 tables and subscriptions
		const NUM_SUBS: usize = 50;
		let mut subs = Vec::new();
		let mut tables = Vec::new();

		for i in 0..NUM_SUBS {
			let table = unique_table_name(&format!("stress_{}", i));
			create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			subs.push(sub);
			tables.push(table);
		}

		// Insert into all tables
		for table in &tables {
			client.command(&format!("INSERT test::{} [{{ id: 1 }}]", table), None).await.unwrap();
		}

		// Receive all notifications (each subscription gets its own stream)
		let mut received = 0;
		for sub in &mut subs {
			let frames = recv_with_timeout(sub, 30000).await;
			if frames.is_some() {
				received += 1;
			}
		}
		assert_eq!(received, NUM_SUBS, "Should receive {} notifications, got {}", NUM_SUBS, received);

		// Cleanup: drop all subscriptions
		drop(subs);
	});

	cleanup_server(Some(server));
}

/// Test many clients connecting concurrently
#[test]
fn test_many_concurrent_clients() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 20;

		// First, create one client to set up the shared table
		let mut setup_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let shared_table = unique_table_name("stress_concurrent");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		drop(setup_client);

		// Track how many clients received their notification
		let received_count = Arc::new(AtomicUsize::new(0));

		// Spawn all clients concurrently
		let mut handles = Vec::new();
		for client_idx in 0..NUM_CLIENTS {
			let table = shared_table.clone();
			let counter = Arc::clone(&received_count);

			let handle = tokio::spawn(async move {
				let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await?;
				client.authenticate("mysecrettoken");

				let mut sub = client.subscribe(&format!("from test::{}", table)).await?;

				// Wait for notification with timeout
				let frames = recv_with_timeout(&mut sub, 10000).await;
				if frames.is_some() {
					counter.fetch_add(1, Ordering::SeqCst);
				}

				drop(sub);
				Ok::<_, Box<dyn Error + Send + Sync>>(())
			});
			handles.push((client_idx, handle));
		}

		// Give clients time to connect and subscribe
		sleep(Duration::from_millis(500)).await;

		// Create a new client to trigger the insert
		let mut trigger_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		trigger_client.authenticate("mysecrettoken");
		trigger_client.command(&format!("INSERT test::{} [{{ id: 999 }}]", shared_table), None).await.unwrap();
		drop(trigger_client);

		// Wait for all clients to complete
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

/// Test rapid subscribe/drop cycles
#[test]
fn test_rapid_subscribe_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("stress_rapid");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		// Rapid subscribe/drop cycles - 100 times
		const NUM_CYCLES: usize = 100;
		for i in 0..NUM_CYCLES {
			let sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			drop(sub);

			// Log progress every 25 cycles
			if (i + 1) % 25 == 0 {
				eprintln!("Completed {} rapid cycles", i + 1);
			}
		}

		// Verify system still works after rapid cycles
		let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
		assert!(sub.subscription_id() > 0, "Should get valid subscription after rapid cycles");

		client.command(&format!("INSERT test::{} [{{ id: 999 }}]", table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "Should still receive changes after {} rapid cycles", NUM_CYCLES);

		drop(sub);
	});

	cleanup_server(Some(server));
}

/// Test that server handles clients disconnecting without explicit cleanup
#[test]
fn test_client_disconnect_without_unsubscribe() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_CLIENTS: usize = 10;

		// Create shared table first
		let mut setup_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let shared_table = unique_table_name("stress_disconnect");
		create_test_table(&setup_client, &shared_table, &[("id", "int4")]).await.unwrap();
		drop(setup_client);

		// Connect multiple clients and subscribe, then disconnect abruptly (drop without cleanup)
		for i in 0..NUM_CLIENTS {
			let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
			client.authenticate("mysecrettoken");
			let _sub = client.subscribe(&format!("from test::{}", shared_table)).await.unwrap();

			// Drop the client and subscription without explicit cleanup
			// This simulates an abrupt disconnect
			drop(_sub);
			drop(client);

			if (i + 1) % 5 == 0 {
				eprintln!("Dropped {} clients abruptly", i + 1);
			}
		}

		// Give server time to clean up disconnected clients
		sleep(Duration::from_millis(500)).await;

		// Server should still be healthy - new clients should be able to connect and subscribe
		let mut new_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		new_client.authenticate("mysecrettoken");

		let mut sub = new_client.subscribe(&format!("from test::{}", shared_table)).await.unwrap();
		assert!(sub.subscription_id() > 0, "New client should be able to subscribe after abrupt disconnects");

		// Insert and verify new client receives notification
		new_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", shared_table), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "New client should receive notification");

		drop(sub);
	});

	cleanup_server(Some(server));
}

/// Test concurrent connect/disconnect cycles
#[test]
fn test_concurrent_connect_disconnect() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		const NUM_TASKS: usize = 10;
		const ITERATIONS_PER_TASK: usize = 5;

		// Create a table for each task to avoid transaction conflicts
		let mut setup_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		setup_client.authenticate("mysecrettoken");

		let mut tables = Vec::new();
		for i in 0..NUM_TASKS {
			let table = unique_table_name(&format!("stress_concurrent_{}", i));
			create_test_table(&setup_client, &table, &[("id", "int4")]).await.unwrap();
			tables.push(table);
		}
		drop(setup_client);

		// Track successful operations
		let success_count = Arc::new(AtomicUsize::new(0));

		// Spawn tasks that continuously connect/disconnect
		let mut handles = Vec::new();
		for task_idx in 0..NUM_TASKS {
			let table = tables[task_idx].clone();
			let counter = Arc::clone(&success_count);

			let handle = tokio::spawn(async move {
				for iter in 0..ITERATIONS_PER_TASK {
					// Retry loop for handling transient transaction conflicts
					let mut retries = 0;
					const MAX_RETRIES: usize = 3;

					loop {
						let mut client =
							GrpcClient::connect(&format!("http://[::1]:{}", port)).await?;
						client.authenticate("mysecrettoken");

						match client.subscribe(&format!("from test::{}", table)).await {
							Ok(sub) => {
								// Small delay to simulate some work
								sleep(Duration::from_millis(10)).await;

								drop(sub);

								counter.fetch_add(1, Ordering::SeqCst);
								break; // Success, exit retry loop
							}
							Err(e) if retries < MAX_RETRIES
								&& e.to_string().contains("TXN_001") =>
							{
								// Transaction conflict, retry after small delay
								retries += 1;
								sleep(Duration::from_millis(10 * retries as u64)).await;
								continue;
							}
							Err(e) => {
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

		// Wait for all tasks to complete
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

		// Verify server is still healthy after all the concurrent activity
		let mut final_client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		final_client.authenticate("mysecrettoken");

		let mut sub = final_client.subscribe(&format!("from test::{}", tables[0])).await.unwrap();
		assert!(sub.subscription_id() > 0, "Server should still accept new subscriptions");

		final_client.command(&format!("INSERT test::{} [{{ id: 1 }}]", tables[0]), None).await.unwrap();

		let frames = recv_with_timeout(&mut sub, 5000).await;
		assert!(frames.is_some(), "Server should still deliver notifications after stress test");

		drop(sub);
	});

	cleanup_server(Some(server));
}

#[test]
fn test_subscribe_receive_unsubscribe_cycles() {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let mut server = create_server_instance(&runtime);
	let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

	runtime.block_on(async {
		let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
		client.authenticate("mysecrettoken");

		let table = unique_table_name("stress_full_cycle");
		create_test_table(&client, &table, &[("id", "int4")]).await.unwrap();

		const NUM_CYCLES: usize = 200;
		for i in 0..NUM_CYCLES {
			let mut sub = client.subscribe(&format!("from test::{}", table)).await.unwrap();
			client.command(&format!("INSERT test::{} [{{ id: {} }}]", table, i), None).await.unwrap();

			let frames = recv_with_timeout(&mut sub, 500).await;
			assert!(frames.is_some(), "Cycle {}: should receive notification", i);

			drop(sub);
		}
	});

	cleanup_server(Some(server));
}
