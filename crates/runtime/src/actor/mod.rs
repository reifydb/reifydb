// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Lightweight actor system: each actor owns its mailbox, processes serially, and replies through a typed
//! channel. `core::actors/` enumerates the actor identities the workspace knows about; this module is what
//! those identities are scheduled and run on.

pub mod context;
pub mod mailbox;
pub mod reply;
pub mod system;
pub mod testing;
pub mod timers;
pub mod traits;
