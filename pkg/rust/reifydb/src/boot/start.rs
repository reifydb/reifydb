// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::{
		EventListener,
		lifecycle::{OnCreateEvent, OnStartEvent},
	},
	interface::{
		EncodableKey, SingleVersionCommandTransaction, SingleVersionQueryTransaction, SingleVersionTransaction,
		SystemVersion, SystemVersionKey,
	},
	log_error,
	value::row::EncodedRowLayout,
};
use reifydb_type::Type;

pub(crate) struct StartEventListener<SVT>
where
	SVT: SingleVersionTransaction,
{
	single: SVT,
}

impl<SVT> StartEventListener<SVT>
where
	SVT: SingleVersionTransaction,
{
	pub(crate) fn new(single: SVT) -> Self {
		Self {
			single,
		}
	}
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl<SVT> EventListener<OnStartEvent> for StartEventListener<SVT>
where
	SVT: SingleVersionTransaction,
{
	fn on(&self, _hook: &OnStartEvent) {
		if let Err(e) = (|| -> crate::Result<()> {
			let layout = EncodedRowLayout::new(&[Type::Uint1]);
			let key = SystemVersionKey {
				version: SystemVersion::Storage,
			}
			.encode();

			let created = self.single.with_command(|tx| match tx.get(&key)? {
				None => {
					let mut row = layout.allocate_row();
					layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
					tx.set(&key, row)?;
					Ok(true)
				}
				Some(single) => {
					let version = layout.get_u8(&single.row, 0);
					assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
					Ok(false)
				}
			})?;

			// the database was never started before
			if created {
				self.trigger_database_creation()
			} else {
				Ok(())
			}
		})() {
			log_error!("Failed to handle OnStart event: {}", e);
		}
	}
}

impl<SVT> StartEventListener<SVT>
where
	SVT: SingleVersionTransaction,
{
	fn trigger_database_creation(&self) -> crate::Result<()> {
		self.single.event_bus().emit(OnCreateEvent {});
		Ok(())
	}
}
