// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Every public entry point that mutates catalog or storage state runs inside a transaction obtained from
//! `reifydb-transaction`. Reading or writing a backend directly defeats MVCC, policy enforcement and CDC
//! capture, all of which assume the engine is the single mediator of those concerns.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_value::Result;

pub mod arena;
pub mod bulk_insert;
pub mod engine;
pub mod environment;
pub mod error;
pub mod expression;
pub mod flow;
pub mod partition;
pub mod policy;
#[cfg(not(reifydb_single_threaded))]
pub mod remote;
pub mod run_tests;
pub mod session;
pub mod subscription;
pub mod test_harness;
pub mod transaction;
pub mod vm;
pub mod watermark;

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
