// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Synchronization primitives abstraction.
//!
//! Provides a unified API for synchronization primitives:
//! - **Native**: Uses parking_lot for high-performance locking
//! - **WASM**: Provides no-op implementations (single-threaded)
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::sync::mutex::Mutex;
//! use reifydb_runtime::sync::rwlock::RwLock;
//!
//! let mutex = Mutex::new(42);
//! let rwlock = RwLock::new(vec![1, 2, 3]);
//! ```

pub mod condvar;
pub mod map;
pub mod mutex;
pub mod rwlock;
