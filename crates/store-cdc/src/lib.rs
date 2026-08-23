// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Block-structured storage for the change data capture log, tiered as an in-memory commit buffer over an
//! LRU of decoded blocks over an append-only persistent tier. The log is keyed by a single monotonic commit
//! version, so the three tiers partition the version space into contiguous ranges rather than a keyspace.
//!
//! Invariant: a block is written once and never rewritten. Retention drops whole blocks off the front, so the
//! persistent tier only ever sees appends and prefix truncation, and a reader never contends with the writer.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod config;
pub mod error;
pub mod flush;
pub mod storage;
pub mod store;
pub mod tier;
pub mod types;

pub struct CdcStoreVersion;

impl HasVersion for CdcStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Block-structured storage for the change data capture log".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
