// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::CommitVersion;

use crate::{
	TransactionId,
	multi::transaction::{version::VersionProvider, *},
};

#[derive(Clone)]
pub enum TransactionKind {
	Current(CommitVersion),
	TimeTravel(CommitVersion),
}

#[derive(Clone)]
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

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.transaction = TransactionKind::TimeTravel(version);
	}
}

impl<L> Drop for TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		if let TransactionKind::Current(version) = self.transaction {
			self.engine.inner.done_query(version);
		}
	}
}
