// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_type::Result;

pub mod arena;
pub mod bulk_insert;
pub mod engine;
pub mod environment;
pub mod error;
pub mod expression;
pub mod ffi;
pub mod flow;
pub(crate) mod interceptor;
pub mod policy;
pub mod procedure;
#[cfg(not(target_arch = "wasm32"))]
pub mod remote;
pub mod run_tests;
pub mod session;
pub mod test_harness;
pub mod test_prelude;
pub mod transaction;
pub mod transform;
pub mod vm;

pub struct EngineVersion;

impl HasVersion for EngineVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Query execution and processing engine module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
