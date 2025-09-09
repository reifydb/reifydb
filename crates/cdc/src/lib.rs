// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod poll;

pub use poll::{PollConsumer, PollConsumerConfig};
use reifydb_core::interface::version::{
	ComponentType, HasVersion, SystemVersion,
};

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
