// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! gRPC server. Accepts requests, hands them to `sub-server` for execution, and streams subscription updates back
//! to clients. The protobuf definitions and generated stubs live alongside the conversion code that maps between
//! gRPC messages and the engine's internal request/response types.
//!
//! Subscription delivery uses gRPC server-streaming so a single subscribe call drains for the lifetime of the
//! subscription; convert handles the round-trip translation between RBCF columnar payloads and protobuf message
//! shapes the wire requires.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(not(reifydb_single_threaded))]
pub mod convert;
#[cfg(not(reifydb_single_threaded))]
pub mod error;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod generated;
#[cfg(not(reifydb_single_threaded))]
pub mod server_state;
#[cfg(not(reifydb_single_threaded))]
pub mod service;
#[cfg(not(reifydb_single_threaded))]
pub mod subscription;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
