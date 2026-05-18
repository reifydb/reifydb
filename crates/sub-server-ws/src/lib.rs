// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! WebSocket server. A single bidirectional channel multiplexes commands and subscriptions: a client opens one
//! socket and can issue multiple in-flight queries and live subscriptions over it without paying per-call connection
//! overhead. Suitable for browsers and long-lived sessions where gRPC is awkward.
//!
//! The protocol module defines the framing on top of WebSocket frames; the handler dispatches each inbound message
//! through `sub-server`'s shared execution path. Outbound subscription updates carry the subscription id so the
//! client can demultiplex.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(not(reifydb_single_threaded))]
pub mod factory;
#[cfg(not(reifydb_single_threaded))]
pub mod handler;
#[cfg(not(reifydb_single_threaded))]
pub mod protocol;
#[cfg(not(reifydb_single_threaded))]
pub mod response;
#[cfg(not(reifydb_single_threaded))]
pub mod subscription;
#[cfg(not(reifydb_single_threaded))]
pub mod subsystem;
