// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{
	ComponentType, HasVersion, SystemVersion,
};
pub use reifydb_type::Result;

pub struct AuthVersion;

impl HasVersion for AuthVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "auth".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Authentication and authorization module"
				.to_string(),
			r#type: ComponentType::Module,
		}
	}
}
