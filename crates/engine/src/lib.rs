// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Query execution and orchestration. The engine consumes a compiled `rql` plan, drives the virtual machine over the
//! storage tier, applies policy enforcement, manages per-query session state, and produces the columnar result the
//! caller observes.
//!
//! Above this crate sit the user-facing surfaces (the SDK, the server subsystems, the subscription/flow runtime); below
//! it sit the storage backends, the catalog, the transaction manager, and the policy evaluator. The engine is the
//! place where a logical query becomes an executed query: instructions are issued to storage, intermediate columns
//! flow through transforms, side effects (writes, schema changes, test runs) are committed under transactional
//! guarantees, and policy checks are interleaved at the points where they can decide what the caller is allowed to
//! see or do.
//!
//! Invariant: every public entry point that mutates catalog or storage state runs inside a transaction obtained from
//! `reifydb-transaction`. Bypassing the transaction layer to read or write directly from a backend defeats MVCC,
//! policy enforcement, and CDC capture - all of which assume the engine is the single mediator of those concerns.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_type::Result;

pub mod arena;
pub mod bulk_insert;
pub mod engine;
pub mod environment;
pub mod error;
pub mod expression;
pub mod flow;
pub mod policy;
#[cfg(not(reifydb_single_threaded))]
pub mod remote;
pub mod run_tests;
pub mod session;
pub mod subscription;
pub mod test_harness;
pub mod test_prelude;
pub mod transaction;
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
