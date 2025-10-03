// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod svl;

use reifydb_core::{
	event::EventBus,
	interface::{SingleVersionTransaction, WithEventBus},
};
use reifydb_store_transaction::TransactionStore;
pub use svl::{SvlCommandTransaction, SvlQueryTransaction, TransactionSvl};

#[repr(u8)]
#[derive(Clone)]
pub enum TransactionSingleVersion {
	SingleVersionLock(TransactionSvl) = 0,
}

impl TransactionSingleVersion {
	pub fn svl(store: TransactionStore, bus: EventBus) -> Self {
		Self::SingleVersionLock(TransactionSvl::new(store, bus))
	}
}

impl TransactionSingleVersion {
	pub fn testing() -> Self {
		Self::SingleVersionLock(TransactionSvl::new(TransactionStore::testing_memory(), EventBus::default()))
	}
}

impl WithEventBus for TransactionSingleVersion {
	fn event_bus(&self) -> &EventBus {
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.event_bus(),
		}
	}
}

impl SingleVersionTransaction for TransactionSingleVersion {
	type Query<'a> = SvlQueryTransaction<'a>;
	type Command<'a> = SvlCommandTransaction<'a>;

	#[inline]
	fn begin_query(&self) -> reifydb_core::Result<Self::Query<'_>> {
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.begin_query(),
		}
	}

	#[inline]
	fn begin_command(&self) -> reifydb_core::Result<Self::Command<'_>> {
		match self {
			TransactionSingleVersion::SingleVersionLock(t) => t.begin_command(),
		}
	}
}
