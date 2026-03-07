// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error, future::Future, sync::Arc, time::Duration};

use reifydb_client::{Frame, FrameColumn, GrpcClient, GrpcSubscription, Value};
use tokio::{runtime::Runtime, time::timeout};

use crate::common::{cleanup_server, create_server_instance, start_server_and_get_grpc_port};

mod basic;
mod data_types;
mod filtered;
mod integration;
mod lifecycle;
mod multiple;
mod notifications;
mod stress;

/// Create a unique test table name to avoid conflicts between tests
pub fn unique_table_name(prefix: &str) -> String {
	use std::time::{SystemTime, UNIX_EPOCH};
	let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	format!("{}_{}", prefix, timestamp % 1_000_000_000)
}

/// Create a test table with given columns in the 'test' namespace
pub async fn create_test_table(
	client: &GrpcClient,
	name: &str,
	columns: &[(&str, &str)],
) -> Result<(), Box<dyn Error>> {
	// Create namespace if needed (ignore error if exists)
	let _ = client.admin("create namespace test", None).await;

	let cols = columns.iter().map(|(name, typ)| format!("{}: {}", name, typ)).collect::<Vec<_>>().join(", ");

	client.admin(&format!("create table test::{} {{ {} }}", name, cols), None).await?;
	Ok(())
}

/// Wait for a change with timeout
pub async fn recv_with_timeout(sub: &mut GrpcSubscription, timeout_ms: u64) -> Option<Vec<Frame>> {
	match timeout(Duration::from_millis(timeout_ms), sub.recv()).await {
		Ok(result) => result,
		Err(_) => None,
	}
}

/// Wait for multiple changes with timeout
pub async fn recv_multiple_with_timeout(sub: &mut GrpcSubscription, count: usize, timeout_ms: u64) -> Vec<Vec<Frame>> {
	let mut results = Vec::new();
	let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

	while results.len() < count {
		let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
		if remaining.is_zero() {
			break;
		}

		match timeout(remaining, sub.recv()).await {
			Ok(Some(frames)) => results.push(frames),
			Ok(None) => break,
			Err(_) => break,
		}
	}

	results
}

/// Find a column by name in a Frame
pub fn find_column<'a>(frame: &'a Frame, name: &str) -> Option<&'a FrameColumn> {
	frame.columns.iter().find(|c| c.name == name)
}

/// Get the _op column value from a change frame (1=insert, 2=update, 3=delete)
pub fn get_op_value(frame: &Frame, row_index: usize) -> Option<u8> {
	find_column(frame, "_op").map(|col| match col.data.get_value(row_index) {
		Value::Uint1(v) => v,
		other => panic!("Expected Uint1 for _op, got {:?}", other),
	})
}

/// Test harness for subscription tests that abstracts away boilerplate
pub struct SubscriptionTestHarness;

impl SubscriptionTestHarness {
	/// Run a subscription test with automatic setup and cleanup
	pub fn run<F, Fut>(test_fn: F)
	where
		F: Fn(TestContext) -> Fut + Send + Sync,
		Fut: Future<Output = Result<(), Box<dyn Error>>>,
	{
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		let mut server = create_server_instance(&runtime);
		let port = start_server_and_get_grpc_port(&runtime, &mut server).unwrap();

		runtime.block_on(async {
			let mut client = GrpcClient::connect(&format!("http://[::1]:{}", port)).await.unwrap();
			client.authenticate("mysecrettoken");

			let ctx = TestContext::new(client);
			test_fn(ctx).await.unwrap();
		});

		cleanup_server(Some(server));
	}
}

/// Context provided to each test with convenience methods
pub struct TestContext {
	pub client: GrpcClient,
	table_prefix: String,
}

impl TestContext {
	fn new(client: GrpcClient) -> Self {
		Self {
			client,
			table_prefix: unique_table_name("t"),
		}
	}

	/// Execute raw RQL command
	pub async fn rql(&self, query: &str) -> Result<(), Box<dyn Error>> {
		self.client.command(query, None).await?;
		Ok(())
	}

	/// Create a table with given columns using RQL directly
	/// Returns the full table name (with prefix for uniqueness)
	pub async fn create_table(&self, name: &str, columns: &str) -> Result<String, Box<dyn Error>> {
		let full_name = format!("{}_{}", self.table_prefix, name);
		let _ = self.client.admin("create namespace test", None).await;
		self.client.admin(&format!("create table test::{} {{ {} }}", full_name, columns), None).await?;
		Ok(full_name)
	}

	/// Subscribe to a table, returns a GrpcSubscription
	pub async fn subscribe(&self, table: &str) -> Result<GrpcSubscription, Box<dyn Error>> {
		let sub = self.client.subscribe(&format!("from test::{}", table)).await?;
		Ok(sub)
	}

	/// Insert rows using RQL: `INSERT test::table [{row1}, {row2}]`
	pub async fn insert(&self, table: &str, rows: &str) -> Result<(), Box<dyn Error>> {
		self.client.command(&format!("INSERT test::{} [{}]", table, rows), None).await?;
		Ok(())
	}

	/// Update rows: `UPDATE test::table { new_vals } FILTER {cond}`
	pub async fn update(&self, table: &str, filter: &str, map: &str) -> Result<(), Box<dyn Error>> {
		self.client
			.command(&format!("UPDATE test::{} {{ {} }} FILTER {{{}}}", table, map, filter), None)
			.await?;
		Ok(())
	}

	/// Delete rows: `DELETE test::table FILTER {cond}`
	pub async fn delete(&self, table: &str, filter: &str) -> Result<(), Box<dyn Error>> {
		self.client.command(&format!("DELETE test::{} FILTER {{{}}}", table, filter), None).await?;
		Ok(())
	}

	/// Receive next change notification with 5s timeout
	pub async fn recv(sub: &mut GrpcSubscription) -> Option<Vec<Frame>> {
		recv_with_timeout(sub, 5000).await
	}
}
