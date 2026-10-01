// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod bucket;
pub mod compress;
pub mod convert;
pub mod device;
pub mod error;
pub mod persist;
pub mod predicate;
pub mod reader;
pub mod scalar;
pub mod selection;
pub mod session;
pub mod snapshot;
pub mod stats;
pub mod store;
#[cfg(feature = "testing")]
pub mod testing;

pub struct ColumnStoreVersion;

impl HasVersion for ColumnStoreVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Columnar block storage for materialized snapshots".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
