// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod auth;
mod catalog;
mod engine;
mod execute;
mod key;
mod params;
mod span;
mod storage;
mod transaction;

use crate::hook::Hooks;
pub use auth::*;
pub use catalog::*;
pub use engine::*;
pub use execute::*;
pub use key::*;
pub use params::*;
pub use span::*;
pub use storage::*;
pub use transaction::*;

pub trait GetHooks {
    fn get_hooks(&self) -> &Hooks;
}
