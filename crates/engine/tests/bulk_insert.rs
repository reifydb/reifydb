// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integration tests for the bulk_insert module.
//!
//! Tests cover all API paths, validation modes, error conditions, and edge cases
//! for the fluent bulk insert API that bypasses RQL parsing for maximum performance.

use reifydb_core::interface::auth::Identity;
use reifydb_engine::engine::StandardEngine;
use reifydb_type::value::frame::frame::Frame;

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

pub fn create_namespace(engine: &StandardEngine, name: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE NAMESPACE {name}"), Default::default()).unwrap();
}

pub fn create_table(engine: &StandardEngine, namespace: &str, table: &str, columns: &str) {
	let identity = test_identity();
	engine.command_as(&identity, &format!("CREATE TABLE {namespace}.{table} {{ {columns} }}"), Default::default())
		.unwrap();
}

pub fn create_ringbuffer(engine: &StandardEngine, namespace: &str, name: &str, capacity: u64, columns: &str) {
	let identity = test_identity();
	engine.command_as(
		&identity,
		&format!("CREATE RINGBUFFER {namespace}.{name} {{ {columns} }} WITH {{ capacity: {capacity} }}"),
		Default::default(),
	)
	.unwrap();
}

pub fn query_table(engine: &StandardEngine, table: &str) -> Vec<Frame> {
	let identity = test_identity();
	engine.query_as(&identity, &format!("FROM {table}"), Default::default()).unwrap()
}

pub fn query_ringbuffer(engine: &StandardEngine, ringbuffer: &str) -> Vec<Frame> {
	let identity = test_identity();
	engine.query_as(&identity, &format!("FROM {ringbuffer}"), Default::default()).unwrap()
}

pub fn test_identity() -> Identity {
	Identity::root()
}

pub fn row_count(frames: &[Frame]) -> usize {
	frames.first().unwrap().rows().count()
}
