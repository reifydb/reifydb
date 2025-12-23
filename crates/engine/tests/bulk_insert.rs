// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Integration tests for the bulk_insert module.
//!
//! Tests cover all API paths, validation modes, error conditions, and edge cases
//! for the fluent bulk insert API that bypasses RQL parsing for maximum performance.

#[path = "bulk_insert/basic.rs"]
mod basic;
#[path = "bulk_insert/coerce.rs"]
mod coerce;
#[path = "bulk_insert/errors.rs"]
mod errors;
#[path = "bulk_insert/ringbuffer.rs"]
mod ringbuffer;
#[path = "bulk_insert/transaction.rs"]
mod transaction;
#[path = "bulk_insert/trusted.rs"]
mod trusted;

use futures_util::TryStreamExt;
use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	Frame,
	event::EventBus,
	interceptor::StandardInterceptorFactory,
	interface::{Engine, Identity},
};
use reifydb_engine::StandardEngine;
use reifydb_store_transaction::TransactionStore;
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};

/// Create a test engine with in-memory storage.
pub async fn create_test_engine() -> StandardEngine {
	let store = TransactionStore::testing_memory().await;
	let eventbus = EventBus::new();
	let single = TransactionSingle::svl(store.clone(), eventbus.clone());
	let cdc = TransactionCdc::new(store.clone());
	let multi = TransactionMulti::new(store, single.clone(), eventbus.clone()).await.unwrap();

	StandardEngine::new(
		multi,
		single,
		cdc,
		eventbus,
		Box::new(StandardInterceptorFactory::default()),
		MaterializedCatalog::new(),
		None,
	)
	.await
}

/// Create a namespace via RQL command.
pub async fn create_namespace(engine: &StandardEngine, name: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE NAMESPACE {name}"), Default::default())
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
}

/// Create a table via RQL command.
/// Syntax: CREATE TABLE ns.name { col: type, ... }
pub async fn create_table(engine: &StandardEngine, namespace: &str, table: &str, columns: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE TABLE {namespace}.{table} {{ {columns} }}"), Default::default())
		.try_collect::<Vec<_>>()
		.await
		.unwrap();
}

/// Create a ringbuffer via RQL command.
/// Syntax: CREATE RINGBUFFER ns.name { col: type, ... } WITH { capacity: n }
pub async fn create_ringbuffer(engine: &StandardEngine, namespace: &str, name: &str, capacity: u64, columns: &str) {
	let identity = test_identity();
	engine.command_as(
		&identity,
		&format!("CREATE RINGBUFFER {namespace}.{name} {{ {columns} }} WITH {{ capacity: {capacity} }}"),
		Default::default(),
	)
	.try_collect::<Vec<_>>()
	.await
	.unwrap();
}

/// Query table contents for verification.
pub async fn query_table(engine: &StandardEngine, table: &str) -> Vec<Frame> {
	let identity = test_identity();
	engine.query_as(&identity, &format!("FROM {table}"), Default::default()).try_collect().await.unwrap()
}

/// Query ringbuffer contents for verification.
pub async fn query_ringbuffer(engine: &StandardEngine, ringbuffer: &str) -> Vec<Frame> {
	let identity = test_identity();
	engine.query_as(&identity, &format!("FROM {ringbuffer}"), Default::default()).try_collect().await.unwrap()
}

/// Default test identity.
pub fn test_identity() -> Identity {
	Identity::root()
}

/// Get total row count from query result.
pub fn row_count(frames: &[Frame]) -> usize {
	frames.first().unwrap().rows().count()
}
