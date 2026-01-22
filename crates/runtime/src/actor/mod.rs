// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Thread-based actor model for ReifyDB.
//!
//! This module provides an actor model that provides identical semantics whether
//! running on a single thread (WASM) or multiple OS threads (native).
//!
//! # Execution Model
//!
//! - **Native**: Each actor runs on its own OS thread using `std::thread::spawn`. Messages are sent via
//!   `crossbeam-channel` and received with blocking `recv()`.
//! - **WASM**: Messages are processed inline (synchronously) when sent. No separate thread or task is created.
//!
//! # Design Goals
//!
//! 1. **Behavioral Equivalence**: Same code, same semantics on 1 thread or N threads
//! 2. **Thread-Based on Native**: Each actor runs on its own OS thread
//! 3. **Synchronous on WASM**: Messages processed inline when sent
//! 4. **No Async/Tokio**: Uses `std::thread` and `crossbeam-channel`, not async
//! 5. **Testability**: Actors can be tested synchronously without spawning threads
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::actor::{Actor, ActorRuntime, Context, Flow};
//!
//! struct Counter;
//!
//! enum CounterMsg {
//!     Inc,
//!     Get(std::sync::mpsc::Sender<i64>),
//! }
//!
//! impl Actor for Counter {
//!     type State = i64;
//!     type Message = CounterMsg;
//!
//!     fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
//!         0
//!     }
//!
//!     fn handle(
//!         &self,
//!         state: &mut Self::State,
//!         msg: Self::Message,
//!         _ctx: &Context<Self::Message>,
//!     ) -> Flow {
//!         match msg {
//!             CounterMsg::Inc => *state += 1,
//!             CounterMsg::Get(tx) => { let _ = tx.send(*state); }
//!         }
//!         Flow::Continue
//!     }
//! }
//!
//! // Create runtime and spawn actor
//! let runtime = ActorRuntime::new();
//! let counter = runtime.spawn_ref("counter", Counter);
//!
//! // Send messages
//! counter.send(CounterMsg::Inc).unwrap();
//! ```
//!
//! # Testing
//!
//! Actors can be tested synchronously using the [`TestHarness`]:
//!
//! ```ignore
//! use reifydb_runtime::actor::{TestHarness, Flow};
//!
//! let mut harness = TestHarness::new(Counter);
//! harness.send(CounterMsg::Inc);
//! harness.send(CounterMsg::Inc);
//! harness.process_all();
//!
//! assert_eq!(*harness.state(), 2);
//! ```

pub mod context;
pub mod mailbox;
pub mod runner;
pub mod runtime;
pub mod testing;
pub mod timers;
pub mod traits;
