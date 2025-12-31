// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{event::EventBus, interface::WithEventBus};

use crate::multi::transaction::TransactionMulti;

impl WithEventBus for TransactionMulti {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}
