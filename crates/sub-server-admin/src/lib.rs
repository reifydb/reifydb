// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Admin server subsystem for ReifyDB.
//!
//! This crate provides an Axum-based HTTP server for web-based administration
//! of ReifyDB. It integrates with the shared tokio runtime and implements the
//! standard ReifyDB `Subsystem` trait for lifecycle management.
//!
//! # Features
//!
//! - REST API for configuration and metrics
//! - Authentication support (optional)
//! - Static file serving for admin UI
//! - Health check endpoint
//! - Graceful shutdown support
//!
//! # Endpoints
//!
//! - `GET /health` - Health check
//! - `POST /v1/auth/login` - Login
//! - `POST /v1/auth/logout` - Logout
//! - `GET /v1/auth/status` - Auth status
//! - `GET /v1/config` - Get configuration
//! - `PUT /v1/config` - Update configuration
//! - `POST /v1/execute` - Execute query
//! - `GET /v1/metrics` - System metrics
//! - `GET /` - Admin UI
//!
//! # Example
//!
//! ```ignore
//! use reifydb_core::SharedRuntime;
//! use reifydb_sub_server_admin::{AdminConfig, AdminSubsystem, AdminState};
//!
//! // Create shared runtime
//! let runtime = SharedRuntime::new(4);
//!
//! // Create application state
//! let state = AdminState::new(engine, 1000, Duration::from_secs(30), false, None);
//!
//! // Create and start admin subsystem
//! let mut admin = AdminSubsystem::with_runtime(
//!     "127.0.0.1:9090".to_string(),
//!     state,
//!     Arc::new(runtime),
//! );
//! admin.start()?;
//! ```

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod assets;
pub mod config;
pub mod factory;
pub mod handlers;
pub mod routes;
pub mod state;
pub mod subsystem;
