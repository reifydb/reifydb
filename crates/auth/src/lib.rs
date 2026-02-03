// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub struct AuthVersion;

impl HasVersion for AuthVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME").strip_prefix("reifydb-").unwrap_or(env!("CARGO_PKG_NAME")).to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Authentication and authorization module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
