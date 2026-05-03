// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::event::EventBus;

use crate::buffer::tier::BufferTier;

#[derive(Clone)]
pub struct SingleStoreConfig {
	pub buffer: Option<BufferConfig>,
	pub event_bus: EventBus,
}

#[derive(Clone)]
pub struct BufferConfig {
	pub storage: BufferTier,
}
