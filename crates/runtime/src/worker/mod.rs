// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Worker thread abstraction for background processing.
//!
//! Provides a unified API for spawning background workers:
//! - **Native**: Spawns OS threads with channel-based message passing
//! - **WASM**: Processes messages synchronously or via async tasks
//!
//! # Example
//!
//! ```ignore
//! #[cfg(feature = "native")]
//! use reifydb_runtime::worker::native::WorkerThread;
//! #[cfg(feature = "wasm")]
//! use reifydb_runtime::worker::wasm::WorkerThread;
//!
//! enum Message {
//!     Process(u64),
//!     Stop,
//! }
//!
//! let worker = WorkerThread::spawn("my-worker".to_string(), |receiver| {
//!     while let Ok(msg) = receiver.recv() {
//!         match msg {
//!             Message::Process(value) => {
//!                 // Process the value
//!             }
//!             Message::Stop => break,
//!         }
//!     }
//! });
//!
//! worker.send(Message::Process(42)).unwrap();
//! worker.send(Message::Stop).unwrap();
//! worker.stop();
//! ```

#[cfg(feature = "native")]
pub mod native;

#[cfg(feature = "wasm")]
pub mod wasm;
