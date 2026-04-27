// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Fluent API for fast bulk inserts into sources.
//!
//! This module provides a builder pattern API that bypasses RQL parsing
//! for maximum insert performance. All inserts within a single builder
//! execute in one transaction (one request = one transaction).
//!
//! # Example
//!
//! ```ignore
//! use reifydb_type::params;
//!
//! engine.bulk_insert(&identity)
//!     .table("namespace::users")
//!         .row(params!{ id: 1, name: "Alice" })
//!         .row(params!{ id: 2, name: "Bob" })
//!         .done()
//!     .ringbuffer("namespace::events")
//!         .row(params!{ timestamp: 12345, event_type: "login" })
//!         .done()
//!     .series("namespace::cpu_usage")
//!         .row(params!{ timestamp: 12345, value: 0.42 })
//!         .done()
//!     .execute()?;
//! ```

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
