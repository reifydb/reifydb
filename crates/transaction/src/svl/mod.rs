// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, RwLock};

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	event::EventBus,
	interface::{Unversioned, UnversionedStorage, UnversionedTransaction, WithEventBus},
	row::EncodedRow,
};

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlReadTransaction;
pub use write::SvlWriteTransaction;

#[derive(Clone)]
pub struct SingleVersionLock<US> {
	inner: Arc<SvlInner<US>>,
}

struct SvlInner<US> {
	storage: RwLock<US>,
	event_bus: EventBus,
}

impl<US> SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	pub fn new(storage: US, event_bus: EventBus) -> Self {
		Self {
			inner: Arc::new(SvlInner {
				storage: RwLock::new(storage),
				event_bus,
			}),
		}
	}
}

impl<US> WithEventBus for SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	fn event_bus(&self) -> &EventBus {
		&self.inner.event_bus
	}
}

impl<US> UnversionedTransaction for SingleVersionLock<US>
where
	US: UnversionedStorage,
{
	type Query<'a> = SvlReadTransaction<'a, US>;
	type Command<'a> = SvlWriteTransaction<'a, US>;

	fn begin_query(&self) -> crate::Result<Self::Query<'_>> {
		let storage = self.inner.storage.read().unwrap();
		Ok(SvlReadTransaction {
			storage,
		})
	}

	fn begin_command(&self) -> crate::Result<Self::Command<'_>> {
		let storage = self.inner.storage.write().unwrap();
		Ok(SvlWriteTransaction::new(storage))
	}
}
