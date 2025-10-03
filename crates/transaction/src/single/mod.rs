// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::{Arc, RwLock};

use reifydb_core::{
	CowVec, EncodedKey, EncodedKeyRange,
	delta::Delta,
	event::EventBus,
	interface::{SingleVersionTransaction, SingleVersionValues, WithEventBus},
	value::encoded::EncodedValues,
};

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlReadTransaction;
use reifydb_store_transaction::SingleVersionStore;
pub use write::SvlWriteTransaction;

#[derive(Clone)]
pub struct SingleVersionLock<SVS> {
	inner: Arc<SvlInner<SVS>>,
}

struct SvlInner<SVS> {
	storage: RwLock<SVS>,
	event_bus: EventBus,
}

impl<SVS> SingleVersionLock<SVS>
where
	SVS: SingleVersionStore,
{
	pub fn new(storage: SVS, event_bus: EventBus) -> Self {
		Self {
			inner: Arc::new(SvlInner {
				storage: RwLock::new(storage),
				event_bus,
			}),
		}
	}
}

impl<SVS> WithEventBus for SingleVersionLock<SVS>
where
	SVS: SingleVersionStore,
{
	fn event_bus(&self) -> &EventBus {
		&self.inner.event_bus
	}
}

impl<SVS> SingleVersionTransaction for SingleVersionLock<SVS>
where
	SVS: SingleVersionStore,
{
	type Query<'a> = SvlReadTransaction<'a, SVS>;
	type Command<'a> = SvlWriteTransaction<'a, SVS>;

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
