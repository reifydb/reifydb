// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Common infrastructure shared by every server transport - gRPC, HTTP, WebSocket, admin. Owns the dispatch loop
//! that accepts an authenticated request, looks up the binding, executes it through the engine, and serialises the
//! result back to the caller. Authentication, format negotiation, response shaping, subscription bookkeeping, and
//! interceptor hooks all live here so the transport-specific crates only need to handle protocol framing.
//!
//! This crate does not bind a socket; protocol-specific crates (`sub-server-grpc`, `sub-server-http`,
//! `sub-server-ws`, `sub-server-admin`) do that and delegate every per-request decision back here.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod actor;
pub mod auth;
pub mod binding;
pub mod dispatch;
pub mod execute;
pub mod format;
pub mod interceptor;
pub mod response;
pub mod state;
pub mod subscribe;
pub mod wire;
