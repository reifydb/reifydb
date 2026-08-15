// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod commit;
pub mod config;
pub mod flush;
pub mod persistent;
pub mod store;
pub mod types;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub mod sqlite;

pub struct OperatorStoreVersion;

impl HasVersion for OperatorStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Single-version storage for flow operator state".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
