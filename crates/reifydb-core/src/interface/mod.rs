// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod auth;
mod catalog;
mod cdc;
mod engine;
pub mod evaluate;
mod execute;
pub mod fragment;
pub mod key;
mod params;
mod storage;
mod transaction;
pub mod worker_pool;

pub use auth::*;
pub use catalog::*;
pub use cdc::*;
pub use engine::*;
pub use evaluate::*;
pub use execute::*;
pub use fragment::*;
pub use key::*;
pub use params::*;
pub use storage::*;
pub use transaction::*;

use crate::hook::Hooks;

pub trait GetHooks {
	fn get_hooks(&self) -> &Hooks;
}
