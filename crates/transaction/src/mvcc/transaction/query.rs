// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{CommitVersion, interface::TransactionId};

use crate::mvcc::transaction::{version::VersionProvider, *};

pub enum TransactionKind {
	Current(CommitVersion),
	TimeTravel(CommitVersion),
}

/// TransactionManagerRx is a read-only transaction manager.
pub struct TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	id: TransactionId,
	engine: TransactionManager<L>,
	transaction: TransactionKind,
}

impl<L> TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	pub fn new_current(id: TransactionId, engine: TransactionManager<L>, version: CommitVersion) -> Self {
		Self {
			id,
			engine,
			transaction: TransactionKind::Current(version),
		}
	}

	pub fn new_time_travel(id: TransactionId, engine: TransactionManager<L>, version: CommitVersion) -> Self {
		Self {
			id,
			engine,
			transaction: TransactionKind::TimeTravel(version),
		}
	}

	pub fn id(&self) -> TransactionId {
		self.id
	}

	pub fn version(&self) -> CommitVersion {
		match self.transaction {
			TransactionKind::Current(version) => version,
			TransactionKind::TimeTravel(version) => version,
		}
	}
}

impl<L> Drop for TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		// time travel transaction have no effect on mvcc
		if let TransactionKind::Current(version) = self.transaction {
			self.engine.inner.done_query(version);
		}
	}
}
