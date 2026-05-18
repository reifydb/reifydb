// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Synchronisation primitives that are mockable under deterministic simulation. The mutex, rwlock, condvar,
//! waiter, and concurrent map exposed here delegate to native equivalents on real targets and to a virtualised
//! scheduler on DST. Code that builds on `std::sync` directly cannot be replayed; code that builds on this module
//! can.

pub mod condvar;
pub mod map;
pub mod mutex;
pub mod rwlock;
pub mod waiter;
