// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
