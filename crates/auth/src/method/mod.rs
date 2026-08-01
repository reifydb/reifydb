// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Authentication method implementations. Each owns its own credential format, verification logic and challenge
//! state, and registers with `registry/`; adding a method means writing one module and registering it.

pub mod github;
pub mod password;
pub mod solana;
pub mod token;
