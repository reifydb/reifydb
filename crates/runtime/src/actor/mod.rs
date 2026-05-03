// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Lightweight actor system. Each actor owns its mailbox, processes messages serially, and replies through a typed
//! channel; the system handle spawns actors onto the runtime's pools and supervises their lifecycle. Timers,
//! testing fixtures, and reply patterns sit alongside so subsystem code can build on a consistent message-passing
//! base without rolling its own concurrency primitives.
//!
//! `core::actors/` enumerates the actor identities the workspace knows about; this module is what those identities
//! are scheduled and run on.

pub mod context;
pub mod mailbox;
pub mod reply;
pub mod system;
pub mod testing;
pub mod timers;
pub mod traits;
