// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! In-memory representation of query results and the structures that carry rows from the engine to the consumer.
//!
//! Holds the columnar data model, the row-oriented presentation layer used by display code, the batch wrapper that
//! packages results for delivery (lazy or fully materialised), and the in-memory index types used by the engine.
//! Together these form the runtime representation that everything above the storage tier (engine, subscriptions, the
//! wire layer, the SDK) operates on.

pub mod batch;
pub mod column;
pub mod frame;
pub mod index;
