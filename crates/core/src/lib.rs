// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod common;
pub mod delta;
pub mod event;
pub mod interceptor;
pub mod interface;
pub mod key;
pub mod retention;
mod row;
mod sort;
pub mod stream;
pub mod util;
pub mod value;

pub use common::*;
pub use interface::TransactionId;
use interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::{Error, Result, async_cow_vec, diagnostic, err, error, return_error, return_internal_error};
pub use row::Row;
pub use sort::{SortDirection, SortKey};
pub use util::{BitVec, CowVec, Either, RetryError, WaitGroup, ioc, retry};
pub use value::{
	encoded::{EncodedKey, EncodedKeyBuilder, EncodedKeyRange},
	frame::*,
};

pub struct CoreVersion;

impl HasVersion for CoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "core".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Core database interfaces and data structures".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
