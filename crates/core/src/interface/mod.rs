// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod auth;
pub mod catalog;
mod cdc;
pub mod evaluate;
mod flow;
mod function;
pub mod identifier;
pub mod resolved;
mod store;
mod transaction;
pub mod version;

pub use auth::*;
pub use catalog::*;
pub use cdc::*;
pub use flow::*;
pub use function::*;
pub use identifier::*;
pub use reifydb_type::{Fragment, Params, StatementColumn, StatementLine};
pub use resolved::*;
pub use store::*;
pub use transaction::*;

use crate::event::EventBus;
pub use crate::key::*;

pub trait WithEventBus {
	fn event_bus(&self) -> &EventBus;
}
