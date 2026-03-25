// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]
extern crate core;

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod challenge;
pub mod error;
pub mod method;
pub mod registry;
pub mod service;

pub struct AuthVersion;

impl HasVersion for AuthVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Authentication and authorization module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
