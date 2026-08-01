// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Fast path for inserting many rows at once: validates the batch up front and writes through the storage commit
//! path instead of the per-row VM dispatch loop. Validation here must match what the per-row INSERT path applies;
//! any divergence lets one path accept rows the other rejects and silently produces inconsistent state.

pub mod builder;
pub mod coerce;
pub mod storage;
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
