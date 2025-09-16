// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	event::{
		lifecycle::{OnCreateEvent, OnStartEvent},
		EventListener,
	},
	interface::{
		EncodableKey, SystemVersion, SystemVersionKey, UnversionedCommandTransaction,
		UnversionedQueryTransaction, UnversionedTransaction,
	},
	log_error,
	row::EncodedRowLayout,
};
use reifydb_type::Type;

pub(crate) struct StartEventListener<UT>
where
	UT: UnversionedTransaction,
{
	unversioned: UT,
}

impl<UT> StartEventListener<UT>
where
	UT: UnversionedTransaction,
{
	pub(crate) fn new(unversioned: UT) -> Self {
		Self {
			unversioned,
		}
	}
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl<UT> EventListener<OnStartEvent> for StartEventListener<UT>
where
	UT: UnversionedTransaction,
{
	fn on(&self, _hook: &OnStartEvent) {
		if let Err(e) = (|| -> crate::Result<()> {
			let layout = EncodedRowLayout::new(&[Type::Uint1]);
			let key = SystemVersionKey {
				version: SystemVersion::Storage,
			}
			.encode();

			let created = self.unversioned.with_command(|tx| match tx.get(&key)? {
				None => {
					let mut row = layout.allocate_row();
					layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
					tx.set(&key, row)?;
					Ok(true)
				}
				Some(unversioned) => {
					let version = layout.get_u8(&unversioned.row, 0);
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

impl<UT> StartEventListener<UT>
where
	UT: UnversionedTransaction,
{
	fn trigger_database_creation(&self) -> crate::Result<()> {
		self.unversioned.event_bus().emit(OnCreateEvent {});
		Ok(())
	}
}
