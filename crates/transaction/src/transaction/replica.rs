// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{MultiVersionBatch, MultiVersionRow},
};
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	TransactionId,
	error::TransactionError,
	multi::{
		pending::PendingWrites,
		transaction::{MultiTransaction, replica::MultiReplicaTransaction},
	},
};

/// A replica transaction for applying replicated catalog changes.
///
/// This is a lean, purpose-built transaction type that commits at the
/// primary's exact version. It has no interceptors, no change tracking,
/// no RQL executor — only the read/write surface needed by
/// `CatalogChangeApplier` implementations.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct ReplicaTransaction {
	pub(crate) multi: MultiTransaction,
	pub(crate) cmd: Option<MultiReplicaTransaction>,
	state: ReplicaTransactionState,
}

#[derive(Clone, Copy, PartialEq)]
enum ReplicaTransactionState {
	Active,
	Committed,
	RolledBack,
}

impl ReplicaTransaction {
	/// Create a new replica transaction at the primary's exact version.
	#[instrument(name = "transaction::replica::new", level = "debug", skip(multi), fields(version = %version.0))]
	pub fn new(multi: MultiTransaction, version: CommitVersion) -> Result<Self> {
		let cmd = multi.begin_replica(version)?;
		Ok(Self {
			multi,
			cmd: Some(cmd),
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

	/// Commit at the primary's exact version.
	///
	/// Bypasses oracle conflict detection, version allocation, and all
	/// interceptors. Does not emit PostCommitEvent.
	#[instrument(name = "transaction::replica::commit_at_version", level = "debug", skip(self))]
	pub fn commit_at_version(&mut self) -> Result<()> {
		self.check_active()?;
		if let Some(mut cmd) = self.cmd.take() {
			self.state = ReplicaTransactionState::Committed;
			cmd.commit_at_version()
		} else {
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	#[instrument(name = "transaction::replica::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<()> {
		self.check_active()?;
		if let Some(mut cmd) = self.cmd.take() {
			self.state = ReplicaTransactionState::RolledBack;
			cmd.rollback()
		} else {
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Get the transaction version (the primary's commit version).
	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.cmd.as_ref().unwrap().version()
	}

	/// Get the transaction ID.
	#[inline]
	pub fn id(&self) -> TransactionId {
		self.cmd.as_ref().unwrap().tm.id()
	}

	/// Get access to the pending writes in this transaction.
	#[inline]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Get a value by key.
	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().get(key)?.map(|v| v.into_multi_version_row()))
	}

	/// Check if a key exists.
	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().contains_key(key)
	}

	/// Get a prefix batch.
	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().prefix(prefix)
	}

	/// Get a reverse prefix batch.
	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().prefix_rev(prefix)
	}

	/// Set a key-value pair.
	#[inline]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().set(key, row)
	}

	/// Unset a key, preserving the deleted values.
	#[inline]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().unset(key, row)
	}

	/// Remove a key without preserving the deleted values.
	#[inline]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().remove(key)
	}

	/// Create a streaming iterator for forward range queries.
	#[inline]
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range(range, batch_size))
	}

	/// Create a streaming iterator for reverse range queries.
	#[inline]
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range_rev(range, batch_size))
	}
}

impl Drop for ReplicaTransaction {
	fn drop(&mut self) {
		if let Some(mut cmd) = self.cmd.take() {
			if self.state == ReplicaTransactionState::Active {
				let _ = cmd.rollback();
			}
		}
	}
}
