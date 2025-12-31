// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use async_trait::async_trait;
use reifydb_core::{
	event::{
		EventListener,
		lifecycle::{OnCreateEvent, OnStartEvent},
	},
	interface::{EncodableKey, SystemVersion, SystemVersionKey, WithEventBus},
	value::encoded::EncodedValuesLayout,
};
use reifydb_transaction::single::TransactionSingle;
use reifydb_type::Type;
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

#[async_trait]
impl EventListener<OnStartEvent> for StartEventListener {
	async fn on(&self, _hook: &OnStartEvent) {
		if let Err(e) = self.handle_start().await {
			error!("Failed to handle OnStart event: {}", e);
		}
	}
}

impl StartEventListener {
	async fn handle_start(&self) -> crate::Result<()> {
		let layout = EncodedValuesLayout::new(&[Type::Uint1]);
		let key = SystemVersionKey {
			version: SystemVersion::Storage,
		}
		.encode();

		// Manually manage transaction since we need async operations
		let mut tx = self.single.begin_command([&key]).await?;

		let created = match tx.get(&key).await? {
			None => {
				let mut row = layout.allocate();
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

		tx.commit().await?;

		// the database was never started before
		if created {
			self.trigger_database_creation().await
		} else {
			Ok(())
		}
	}

	async fn trigger_database_creation(&self) -> crate::Result<()> {
		self.single.event_bus().emit(OnCreateEvent {}).await;
		Ok(())
	}
}
