// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Ephemeral subscription subsystem: maintains the per-client cursor that drains the CDC stream and pushes deltas
//! through the connected sink. "Ephemeral" because nothing here persists subscription state across restarts; if a
//! consumer disconnects, its subscription is gone. Durable subscriptions belong elsewhere.
//!
//! The subsystem owns the consumer/poller/sink trio: the consumer holds the engine-side state, the poller drives it
//! forward against the CDC log, and the sink hands deltas to whichever transport the client connected over (gRPC,
//! HTTP, WebSocket, in-process listener).

pub mod consumer;
pub mod poller;
pub mod sink;
pub mod store;
pub mod subsystem;
