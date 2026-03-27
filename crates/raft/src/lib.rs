// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

//! Implements the Raft distributed consensus protocol.
//!
//! Ported from toydb's Raft implementation and adapted for ReifyDB's types.
//! The Raft node is a pure state machine driven by `step(message)` and
//! `tick()`, with zero I/O. Outbound messages are collected in an outbox
//! and drained by the caller after each transition.

pub mod log;
pub mod message;
pub mod node;
pub mod state;

pub use log::{Entry, Index, Log};
pub use message::{Command, Envelope, Message};
pub use node::{Node, NodeId, Options, Progress, Term, Ticks};
pub use state::{KVState, State, test_write};
