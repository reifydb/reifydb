// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Synchronisation primitives that are mockable under deterministic simulation. Code that builds on `std::sync`
//! directly cannot be replayed; code that builds on this module can.

pub mod condvar;
pub mod map;
pub mod mutex;
pub mod rwlock;
pub mod waiter;
