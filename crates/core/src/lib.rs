// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use crate::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod common;
pub mod delta;
pub mod encoded;
pub mod event;
pub mod interface;
pub mod key;
pub mod retention;
pub mod row;
pub mod runtime;
pub use encoded::schema;
pub mod sort;
pub mod util;
pub mod value;

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
