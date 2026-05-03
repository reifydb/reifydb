// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::{
		change::Change,
		store::{MultiVersionBatch, MultiVersionRow},
	},
};
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	TransactionId,
	change::RowChange,
	error::TransactionError,
	multi::{
		pending::PendingWrites,
		transaction::{MultiTransaction, replica::MultiReplicaTransaction},
	},
	transaction::write::Write,
};

pub struct ReplicaTransaction {
	pub(crate) rpl: Option<MultiReplicaTransaction>,
	state: ReplicaTransactionState,
}

#[derive(Clone, Copy, PartialEq)]
enum ReplicaTransactionState {
	Active,
	Committed,
	RolledBack,
}

impl ReplicaTransaction {
	#[instrument(name = "transaction::replica::new", level = "debug", skip(multi), fields(version = %version.0))]
	pub fn new(multi: MultiTransaction, version: CommitVersion) -> Result<Self> {
		let rpl = multi.begin_replica(version)?;
		Ok(Self {
			rpl: Some(rpl),
			state: ReplicaTransactionState::Active,
		})
	}

	fn check_active(&self) -> Result<()> {
		match self.state {
			ReplicaTransactionState::Active => Ok(()),
			ReplicaTransactionState::Committed => Err(TransactionError::AlreadyCommitted.into()),
			ReplicaTransactionState::RolledBack => Err(TransactionError::AlreadyRolledBack.into()),
		}
	}

	#[instrument(name = "transaction::replica::commit_at_version", level = "debug", skip(self))]
	pub fn commit_at_version(&mut self) -> Result<()> {
		self.check_active()?;
		if let Some(mut cmd) = self.rpl.take() {
			self.state = ReplicaTransactionState::Committed;
			cmd.commit_at_version()
		} else {
			unreachable!("Transaction state inconsistency")
		}
	}

	#[instrument(name = "transaction::replica::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<()> {
		self.check_active()?;
		if let Some(mut cmd) = self.rpl.take() {
			self.state = ReplicaTransactionState::RolledBack;
			cmd.rollback()
		} else {
			unreachable!("Transaction state inconsistency")
		}
	}

	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.rpl.as_ref().unwrap().version()
	}

	#[inline]
	pub fn id(&self) -> TransactionId {
		self.rpl.as_ref().unwrap().id()
	}

	#[inline]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.rpl.as_ref().unwrap().pending_writes()
	}

	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		self.check_active()?;
		Ok(self.rpl.as_mut().unwrap().get(key)?.map(|v| v.into_multi_version_row()))
	}

	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().contains_key(key)
	}

	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().prefix(prefix)
	}

	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().prefix_rev(prefix)
	}

	#[inline]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().set(key, row)
	}

	#[inline]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().unset(key, row)
	}

	#[inline]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().remove(key)
	}

	#[inline]
	pub fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_active()?;
		self.rpl.as_mut().unwrap().mark_preexisting(key);
		Ok(())
	}

	#[inline]
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.rpl.as_mut().unwrap().range(range, batch_size))
	}

	#[inline]
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.rpl.as_mut().unwrap().range_rev(range, batch_size))
	}
}

impl Drop for ReplicaTransaction {
	fn drop(&mut self) {
		if let Some(mut cmd) = self.rpl.take()
			&& self.state == ReplicaTransactionState::Active
		{
			let _ = cmd.rollback();
		}
	}
}

impl Write for ReplicaTransaction {
	#[inline]
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		ReplicaTransaction::set(self, key, row)
	}
	#[inline]
	fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		ReplicaTransaction::unset(self, key, row)
	}
	#[inline]
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		ReplicaTransaction::remove(self, key)
	}
	#[inline]
	fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()> {
		ReplicaTransaction::mark_preexisting(self, key)
	}
	#[inline]
	fn track_row_change(&mut self, _change: RowChange) {}
	#[inline]
	fn track_flow_change(&mut self, _change: Change) {}
}
