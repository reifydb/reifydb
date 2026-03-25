// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
