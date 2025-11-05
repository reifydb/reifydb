// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

mod checkpoint;
mod consumer;
mod poll;
mod watermark;

pub use checkpoint::CdcCheckpoint;
pub use consumer::{CdcConsume, CdcConsumer};
pub use poll::{PollConsumer, PollConsumerConfig};
pub use watermark::compute_watermark;

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
