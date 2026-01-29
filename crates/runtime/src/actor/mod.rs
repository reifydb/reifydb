// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor model for ReifyDB.
//!
//! This module provides an actor model that provides identical semantics whether
//! running on a single thread (WASM) or multiple OS threads (native).
//!
//! # Execution Model
//!
//! - **Native**: Actors run on threads with shared rayon pool for compute
//! - **WASM**: Messages are processed inline (synchronously) when sent
//!
//! All actor states must be `Send`.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_runtime::{system::ActorSystem, actor::{Actor, Context, Flow, ActorConfig}};
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
//! // Create system and spawn actor
//! let system = ActorSystem::new(Default::default());
//! let handle = system.spawn("counter", Counter);
//!
//! // Send messages
//! handle.actor_ref().send(CounterMsg::Inc).unwrap();
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
pub mod system;
pub mod testing;
pub mod timers;
pub mod traits;
