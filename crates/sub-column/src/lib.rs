// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Storage-materialization subsystem: the actor that turns committed deltas into materialized columns in the columnar
//! store. Subscribes to CDC, batches writes per shape, drives the encoders in `column/`, and updates the catalog
//! registry so newly-written columns are visible to readers.
//!
//! The subsystem owns the trade-off between materialization latency and write amplification - too aggressive and
//! every commit pays a heavy column rewrite; too lazy and reads of recent data fall back to row-shaped storage. The
//! actor's policy is what decides when to flush and how to coalesce successive deltas into a single column rewrite.

pub mod actor;
pub mod error;
pub mod factory;
pub mod subsystem;
