// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Fast path for inserting many rows at once. Skips the per-row VM dispatch loop, coerces and validates the input
//! batch up front, and writes directly through the storage commit path so a load of a million rows pays a small
//! constant overhead rather than a million instruction-dispatch costs.
//!
//! Used by ingestion endpoints, replication, and the bulk-load admin tool. Validation here matches the constraints
//! the per-row INSERT path applies; if the two diverge, bulk-insert can accept rows that single-row INSERT would
//! reject (or vice versa), which silently produces inconsistent state.

pub mod builder;
pub mod coerce;
pub mod primitive;
pub mod validation;

#[derive(Debug, Clone, Default)]
pub struct BulkInsertResult {
	pub tables: Vec<TableInsertResult>,
	pub ringbuffers: Vec<RingBufferInsertResult>,
	pub series: Vec<SeriesInsertResult>,
}

#[derive(Debug, Clone)]
pub struct TableInsertResult {
	pub namespace: String,
	pub table: String,
	pub inserted: u64,
}

#[derive(Debug, Clone)]
pub struct RingBufferInsertResult {
	pub namespace: String,
	pub ringbuffer: String,
	pub inserted: u64,
}

#[derive(Debug, Clone)]
pub struct SeriesInsertResult {
	pub namespace: String,
	pub series: String,
	pub inserted: u64,
}
