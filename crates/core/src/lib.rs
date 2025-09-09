// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod common;
pub mod delta;
pub mod event;
pub mod flow;
pub mod index;
pub mod interceptor;
pub mod interface;
pub mod result;
pub mod row;
mod sort;
pub mod util;
pub mod value;

pub use common::*;
use interface::version::{ComponentType, HasVersion, SystemVersion};
pub use interface::{ColumnDescriptor, TransactionId};
pub use result::*;
pub use row::{EncodedKey, EncodedKeyRange};
pub use sort::{SortDirection, SortKey};
pub use util::{BitVec, CowVec, Either, WaitGroup, ioc, retry};

pub struct CoreVersion;

impl HasVersion for CoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "core".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description:
				"Core database interfaces and data structures"
					.to_string(),
			r#type: ComponentType::Module,
		}
	}
}
