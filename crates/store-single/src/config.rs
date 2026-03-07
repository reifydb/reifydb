// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::EventBus;

use crate::hot::tier::HotTier;

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub hot: Option<HotConfig>,
	pub event_bus: EventBus,
}

#[derive(Clone)]
pub struct HotConfig {
	pub storage: HotTier,
}
