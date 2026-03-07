// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{event::EventBus, interface::WithEventBus};

use crate::multi::transaction::MultiTransaction;

impl WithEventBus for MultiTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}
