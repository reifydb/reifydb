// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Common infrastructure for HTTP and WebSocket server subsystems.
//!
//! This crate provides shared types and utilities used by `sub-server-http` and
//! `sub-server-ws`. It includes:
//!
//! - **Authentication**: Identity extraction from headers and tokens
//! - **Execution**: Async wrappers around synchronous database operations
//! - **Response**: Frame conversion for JSON serialization
//! - **Runtime**: Shared tokio runtime management
//! - **State**: Application state for request handler

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod auth;
pub mod execute;
pub mod response;
pub mod state;
