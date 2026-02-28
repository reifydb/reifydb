// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

//! Protocol-agnostic subscription consumption for ReifyDB.
//!
//! This crate provides the core subscription polling and consumption logic,
//! decoupled from any specific transport protocol (WebSocket, HTTP, etc.).

pub mod consumer;
pub mod cursor;
pub mod delivery;
pub mod poller;
pub mod state;
