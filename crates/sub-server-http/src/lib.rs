// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! HTTP server subsystem for ReifyDB.
//!
//! This crate provides an Axum-based HTTP server for handling query and command
//! requests. It integrates with the shared tokio runtime and implements the
//! standard ReifyDB `Subsystem` trait for lifecycle management.
//!
//! # Features
//!
//! - REST API for query and command execution
//! - Bearer token and API key authentication
//! - Request timeouts and connection limits
//! - Graceful shutdown support
//! - Health check endpoint
//!
//! # Endpoints
//!
//! - `GET /health` - Health check (no authentication required)
//! - `POST /v1/query` - Execute read-only queries
//! - `POST /v1/command` - Execute write commands
//!
//! # Example
//!
//! ```ignore
//! use reifydb_sub_server::{AppState, QueryConfig, SharedRuntime};
//! use reifydb_sub_server_http::HttpSubsystem;
//!
//! // Create shared runtime
//! let runtime = SharedRuntime::new(4);
//!
//! // Create application state
//! let state = AppState::new(engine, QueryConfig::default());
//!
//! // Create and start HTTP subsystem
//! let mut http = HttpSubsystem::new(
//!     "0.0.0.0:8090".to_string(),
//!     state,
//!     runtime.handle(),
//! );
//! http.start()?;
//! ```

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod error;
pub mod factory;
pub mod handlers;
pub mod routes;
pub mod subsystem;

pub use error::{AppError, ErrorResponse};
pub use factory::{HttpConfig, HttpSubsystemFactory};
pub use handlers::{QueryResponse, StatementRequest};
pub use routes::router;
pub use subsystem::HttpSubsystem;
