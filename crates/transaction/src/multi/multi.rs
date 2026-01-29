// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{event::EventBus, interface::WithEventBus};

use crate::multi::transaction::MultiTransaction;

impl WithEventBus for MultiTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}
