// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub mod error;
pub mod produce;
pub mod storage;
pub mod consume;

pub use error::{CdcError, CdcResult};
pub use produce::{CdcWorker, CdcWorkItem, CdcEventListener};
pub use storage::{CdcStorage, CdcStorageResult, CdcStore, MemoryCdcStorage};
pub use consume::{
	CdcCheckpoint, CdcConsume, CdcConsumer, CdcHost, PollConsumer, PollConsumerConfig,
	compute_watermark,
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
