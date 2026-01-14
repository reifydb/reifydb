// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::EventBus;

use crate::hot::HotTier;

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub hot: Option<HotConfig>,
	pub event_bus: EventBus,
}

#[derive(Clone)]
pub struct HotConfig {
	pub storage: HotTier,
}

impl Default for SingleStoreConfig {
	fn default() -> Self {
		Self {
			hot: None,
			event_bus: EventBus::new(),
		}
	}
}
