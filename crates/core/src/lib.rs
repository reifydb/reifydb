// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use crate::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod common;
pub mod config;
pub mod delta;
pub mod encoded;
pub mod error;
pub mod event;
pub mod interface;
pub mod key;
pub mod retention;
pub mod row;
pub mod sort;
pub mod testing;
pub mod util;
pub mod value;

pub struct CoreVersion;

impl HasVersion for CoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Core database interfaces and data structures".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
