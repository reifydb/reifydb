// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Authentication method implementations. Each method is self-contained - it owns its credential format,
//! verification logic, and challenge state - and registers with the `registry/` so the service can pick the
//! right one for an incoming request. Adding a new method (a new IDP, a new key type) means writing one of these
//! modules and registering it; the rest of the auth surface is method-agnostic.

pub mod password;
pub mod solana;
pub mod token;
