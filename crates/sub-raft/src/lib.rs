// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

//! Raft distributed consensus protocol for ReifyDB.
//!
//! The core state machine (node, log, message, state) is a pure, zero-I/O
//! implementation driven by `step(message)` and `tick()`. The transport and
//! driver modules connect it to the network and storage layers.

pub mod config;
pub mod driver;
pub mod generated;
pub mod grpc;
pub mod log;
pub mod message;
pub mod node;
pub mod proposal;
pub mod state;
pub mod transport;
