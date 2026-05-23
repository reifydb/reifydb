// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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

pub struct TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	id: TransactionId,
	engine: TransactionManager<L>,
	transaction: TransactionKind,
	registered: Option<CommitVersion>,
}

impl<L> Clone for TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	fn clone(&self) -> Self {
		Self {
			id: self.id,
			engine: self.engine.clone(),
			transaction: self.transaction.clone(),
			registered: None,
		}
	}
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
			registered: Some(version),
		}
	}

	pub fn new_time_travel(id: TransactionId, engine: TransactionManager<L>, version: CommitVersion) -> Self {
		Self {
			id,
			engine,
			transaction: TransactionKind::TimeTravel(version),
			registered: None,
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
		if let Some(version) = self.registered.take() {
			self.engine.inner.done_query(version);
		}
	}
}
