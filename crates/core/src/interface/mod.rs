// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod auth;
pub mod catalog;
mod cdc;
mod engine;
mod execute;
pub mod identifier;
pub mod resolved;
mod store;
mod transaction;
pub mod version;

pub use auth::*;
pub use catalog::*;
pub use cdc::*;
pub use engine::*;
pub use execute::*;
pub use identifier::*;
pub use reifydb_type::{
	BorrowedFragment, Fragment, IntoFragment, LazyFragment, OwnedFragment, Params, StatementColumn, StatementLine,
};
pub use resolved::*;
pub use store::*;
pub use transaction::*;

use crate::event::EventBus;
pub use crate::key::*;

pub trait WithEventBus {
	fn event_bus(&self) -> &EventBus;
}
