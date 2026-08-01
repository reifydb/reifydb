// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! HTTP/JSON server on Axum: request/response and subscribe-via-streaming for clients that do not want a protobuf
//! dependency, on top of `sub-server`'s shared dispatch. This is the canonical place RBCF gets rendered into JSON;
//! anything needing raw RBCF should use the gRPC or WebSocket transports.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(not(reifydb_single_threaded))]
pub mod error;
#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod handlers;
#[cfg(not(reifydb_single_threaded))]
pub mod routes;
#[cfg(not(reifydb_single_threaded))]
pub mod state;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
