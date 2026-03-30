// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

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
#![allow(clippy::tabs_in_doc_comments)]

pub mod auth;
pub mod execute;
pub mod interceptor;
pub mod remote;
pub mod response;
pub mod state;
pub mod subscribe;
pub mod wire;
