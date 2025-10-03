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
use reifydb_store_transaction::TransactionStore;

pub(crate) mod range;
pub(crate) mod range_rev;
mod read;
pub(crate) mod scan;
pub(crate) mod scan_rev;
mod write;

pub use read::SvlQueryTransaction;
pub use write::SvlCommandTransaction;

#[derive(Clone)]
pub struct TransactionSvl {
	inner: Arc<TransactionSvlInner>,
}

struct TransactionSvlInner {
	store: RwLock<TransactionStore>,
	event_bus: EventBus,
}

impl TransactionSvl {
	pub fn new(store: TransactionStore, event_bus: EventBus) -> Self {
		Self {
			inner: Arc::new(TransactionSvlInner {
				store: RwLock::new(store),
				event_bus,
			}),
		}
	}
}

impl WithEventBus for TransactionSvl {
	fn event_bus(&self) -> &EventBus {
		&self.inner.event_bus
	}
}

impl SingleVersionTransaction for TransactionSvl {
	type Query<'a> = SvlQueryTransaction<'a>;
	type Command<'a> = SvlCommandTransaction<'a>;

	fn begin_query(&self) -> crate::Result<Self::Query<'_>> {
		let storage = self.inner.store.read().unwrap();
		Ok(SvlQueryTransaction {
			store: storage,
		})
	}

	fn begin_command(&self) -> crate::Result<Self::Command<'_>> {
		let storage = self.inner.store.write().unwrap();
		Ok(SvlCommandTransaction::new(storage))
	}
}
