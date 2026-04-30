// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod builder;
pub mod coerce;
pub mod primitive;
pub mod validation;

/// Result of a bulk insert operation
#[derive(Debug, Clone, Default)]
pub struct BulkInsertResult {
	pub tables: Vec<TableInsertResult>,
	pub ringbuffers: Vec<RingBufferInsertResult>,
	pub series: Vec<SeriesInsertResult>,
}

/// Result of inserting into a specific table
#[derive(Debug, Clone)]
pub struct TableInsertResult {
	pub namespace: String,
	pub table: String,
	pub inserted: u64,
}

/// Result of inserting into a specific ring buffer
#[derive(Debug, Clone)]
pub struct RingBufferInsertResult {
	pub namespace: String,
	pub ringbuffer: String,
	pub inserted: u64,
}

/// Result of inserting into a specific series
#[derive(Debug, Clone)]
pub struct SeriesInsertResult {
	pub namespace: String,
	pub series: String,
	pub inserted: u64,
}
