// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Cross-crate trait surface. Each submodule defines the contract for one subsystem of the database.
//!
//! Each submodule exposes the traits and value types that downstream crates implement and consume - covering the
//! catalog, authentication, storage, evaluation, dataflow, change-data-capture, change-records, qualified identifiers,
//! name-resolution results, and component versions. The `WithEventBus` trait at the top level is implemented by
//! anything that owns or borrows the in-process event bus, so handler code can publish without holding a direct
//! reference.
//!
//! Invariant: traits in this module are object-safe wherever a downstream crate handles them as `dyn Trait`. The
//! supervision and wiring layer holds erased references to storage backends, evaluators, and catalog stores;
//! introducing a generic, an `async fn`, or a `Self`-typed return on such a trait silently breaks every consumer at the
//! next compile.

use crate::event::EventBus;

pub mod auth;
pub mod catalog;
pub mod cdc;
pub mod change;
pub mod evaluate;
pub mod flow;
pub mod identifier;
pub mod resolved;
pub mod store;
pub mod version;

pub trait WithEventBus {
	fn event_bus(&self) -> &EventBus;
}
