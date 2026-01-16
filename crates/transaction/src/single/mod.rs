// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod svl;

use reifydb_core::{encoded::key::EncodedKey, event::EventBus, interface::WithEventBus};
use reifydb_store_single::SingleStore;

use crate::single::svl::{TransactionSvl, read::SvlQueryTransaction, write::SvlCommandTransaction};

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionSingle {
	SingleVersionLock(TransactionSvl) = 0,
}

impl TransactionSingle {
	pub fn svl(store: SingleStore, bus: EventBus) -> Self {
		Self::SingleVersionLock(TransactionSvl::new(store, bus))
	}

	pub fn testing() -> Self {
		Self::SingleVersionLock(TransactionSvl::new(SingleStore::testing_memory(), EventBus::default()))
	}

	/// Helper for single-version queries.
	pub fn with_query<'a, I, F, R>(&self, keys: I, f: F) -> reifydb_type::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut SvlQueryTransaction<'_>) -> reifydb_type::Result<R>,
	{
		let mut tx = self.begin_query(keys)?;
		f(&mut tx)
	}

	/// Helper for single-version commands.
	pub fn with_command<'a, I, F, R>(&self, keys: I, f: F) -> reifydb_type::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut SvlCommandTransaction<'_>) -> reifydb_type::Result<R>,
	{
		let mut tx = self.begin_command(keys)?;
		let result = f(&mut tx)?;
		tx.commit()?;
		Ok(result)
	}

	#[inline]
	pub fn begin_query<'a, I>(&self, keys: I) -> reifydb_type::Result<SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		match self {
			TransactionSingle::SingleVersionLock(t) => t.begin_query(keys),
		}
	}

	#[inline]
	pub fn begin_command<'a, I>(&self, keys: I) -> reifydb_type::Result<SvlCommandTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		match self {
			TransactionSingle::SingleVersionLock(t) => t.begin_command(keys),
		}
	}
}

impl WithEventBus for TransactionSingle {
	fn event_bus(&self) -> &EventBus {
		match self {
			TransactionSingle::SingleVersionLock(t) => t.event_bus(),
		}
	}
}
