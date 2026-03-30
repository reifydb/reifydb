// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

//! Protocol-agnostic subscription consumption for ReifyDB.
//!
//! This crate provides the core subscription polling and consumption logic,
//! decoupled from any specific transport protocol (WebSocket, HTTP, etc.).

pub mod consumer;
pub mod cursor;
pub mod delivery;
pub mod poller;
pub mod state;
