// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! HTTP/JSON server hosted on top of Axum. Exposes the request/response and subscribe-via-streaming endpoints in
//! a transport that is friendlier than gRPC for browsers, scripting languages, and tooling that does not want to
//! depend on protobuf. Routing, handlers, and per-route state sit on top of `sub-server`'s shared dispatch.
//!
//! JSON payloads come from `wire-format`'s JSON encoder; the HTTP path is the canonical place RBCF gets rendered
//! into something a generic client can consume. Anything that needs raw RBCF should use the gRPC or WebSocket
//! transports instead.

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
