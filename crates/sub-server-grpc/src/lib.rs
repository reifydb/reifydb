// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! gRPC server: accepts requests, hands them to `sub-server` for execution, and streams subscription updates back.
//! Delivery uses server-streaming, so one subscribe call drains for the lifetime of the subscription, and convert
//! maps between RBCF columnar payloads and the protobuf shapes the wire requires.

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
