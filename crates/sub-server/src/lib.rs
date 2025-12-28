// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! Common infrastructure for HTTP and WebSocket server subsystems.
//!
//! This crate provides shared types and utilities used by `sub-server-http` and
//! `sub-server-ws`. It includes:
//!
//! - **Authentication**: Identity extraction from headers and tokens
//! - **Execution**: Async wrappers around synchronous database operations
//! - **Response**: Frame conversion for JSON serialization
//! - **Runtime**: Shared tokio runtime management
//! - **State**: Application state for request handlers

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod auth;
mod execute;
mod response;
mod state;

// Authentication exports
pub use auth::{
	AuthError, AuthResult, anonymous_identity, extract_identity_from_api_key, extract_identity_from_auth_header,
	extract_identity_from_ws_auth, root_identity,
};
// Query execution exports
pub use execute::{
	ExecuteError, ExecuteResult, execute_command, execute_command_single, execute_query, execute_query_single,
};
// Response conversion exports
pub use response::{ResponseColumn, ResponseFrame, convert_frames};
// State exports
pub use state::{AppState, QueryConfig};
