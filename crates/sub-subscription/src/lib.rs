// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Ephemeral subscription subsystem: maintains the per-client cursor that drains the CDC stream and pushes deltas
//! through the connected sink. Nothing here survives a restart - a disconnected consumer's subscription is gone,
//! and durable subscriptions belong elsewhere.

pub mod consumer;
pub mod delivery;
pub mod poller;
pub mod store;
pub mod subsystem;
pub mod tracker;
pub mod transaction;
pub mod watermark;
pub mod worker;
