// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod common;
pub mod compute;
pub mod delta;
pub mod event;
pub mod interface;
pub mod key;
pub mod retention;
mod row;
mod sort;
pub mod util;
pub mod value;

pub use common::*;
pub use compute::ComputePool;
use interface::version::{ComponentType, HasVersion, SystemVersion};
pub use reifydb_type::{Error, Result, diagnostic, err, error, return_error, return_internal_error};
pub use row::Row;
pub use sort::{SortDirection, SortKey};
pub use util::{BitVec, CowVec, Either, RetryError, WaitGroup, ioc, retry};
pub use value::{
	batch::{Batch, LazyBatch, LazyColumnMeta},
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
