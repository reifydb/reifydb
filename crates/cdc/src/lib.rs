// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod consume;
pub mod error;
pub mod produce;
pub mod storage;

pub struct CdcVersion;

impl HasVersion for CdcVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "cdc".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Change Data Capture module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
