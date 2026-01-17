// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::layout::EncodedValuesLayout,
	event::{
		EventListener,
		lifecycle::{OnCreateEvent, OnStartEvent},
	},
	interface::WithEventBus,
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_transaction::single::TransactionSingle;
use reifydb_type::value::r#type::Type;
use tracing::error;

pub(crate) struct StartEventListener {
	single: TransactionSingle,
}

impl StartEventListener {
	pub(crate) fn new(single: TransactionSingle) -> Self {
		Self {
			single,
		}
	}
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl EventListener<OnStartEvent> for StartEventListener {
	fn on(&self, _hook: &OnStartEvent) {
		if let Err(e) = self.handle_start() {
			error!("Failed to handle OnStart event: {}", e);
		}
	}
}

impl StartEventListener {
	fn handle_start(&self) -> crate::Result<()> {
		let layout = EncodedValuesLayout::new(&[Type::Uint1]);
		let key = SystemVersionKey {
			version: SystemVersion::Storage,
		}
		.encode();

		// Manually manage transaction since we need async operations
		let mut tx = self.single.begin_command([&key])?;

		let created = match tx.get(&key)? {
			None => {
				let mut row = layout.allocate_deprecated();
				layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
				tx.set(&key, row)?;
				true
			}
			Some(single) => {
				let version = layout.get_u8(&single.values, 0);
				assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
				false
			}
		};

		tx.commit()?;

		// the database was never started before
		if created {
			self.trigger_database_creation()
		} else {
			Ok(())
		}
	}

	fn trigger_database_creation(&self) -> crate::Result<()> {
		self.single.event_bus().emit(OnCreateEvent {});
		Ok(())
	}
}
