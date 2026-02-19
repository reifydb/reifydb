// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebSocket server subsystem for ReifyDB.
//!
//! This crate provides a tokio-tungstenite-based WebSocket server for handling
//! persistent query connections. It integrates with the shared tokio runtime and
//! implements the transaction ReifyDB `Subsystem` trait for lifecycle management.
//!
//! # Features
//!
//! - Full WebSocket protocol support (RFC 6455)
//! - Authentication via initial Auth message
//! - Query and command execution over persistent connections
//! - Subscription support for real-time push notifications
//! - Connection limits via semaphore
//! - Graceful shutdown with connection draining
//!
//! # Message Protocol
//!
//! All messages are JSON-formatted with the following structure:
//!
//! ```json
//! {
//!   "id": "unique-request-id",
//!   "type": "Auth|Query|Command|Subscribe|Unsubscribe",
//!   "payload": { ... }
//! }
//! ```
//!
//! Server-initiated push messages have this structure:
//!
//! ```json
//! {
//!   "type": "Change",
//!   "payload": { "subscription_id": "...", ... }
//! }
//! ```
//!
//! # Example
//!
//! ```ignore
//! use reifydb_core::SharedRuntime;
//! use reifydb_sub_server::{AppState, QueryConfig};
//! use reifydb_sub_server_ws::WsSubsystem;
//!
//! // Create shared runtime
//! let runtime = SharedRuntime::new(4);
//!
//! // Create application state
//! let state = AppState::new(pool, engine, QueryConfig::default());
//!
//! // Create and start WebSocket subsystem
//! let mut ws = WsSubsystem::new(
//!     "0.0.0.0:8091".to_string(),
//!     state,
//!     runtime.handle(),
//! );
//! ws.start()?;
//! ```

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod factory;
pub mod handler;
pub mod protocol;
pub mod response;
pub mod subscription;
pub mod subsystem;
