// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! WebSocket server. One bidirectional socket multiplexes multiple in-flight commands and live subscriptions, so a
//! long-lived session pays no per-call connection overhead. Outbound subscription updates carry the subscription id
//! so the client can demultiplex.

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
