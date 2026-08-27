// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::common::CommitVersion;
use reifydb_value::Result;
use tracing::instrument;

use crate::transaction::command::CommandTransaction;

pub type CommitApply = Box<dyn FnOnce(&mut CommandTransaction) -> Result<()> + Send>;

pub type CommitCompletion = Box<dyn FnOnce(Result<CommitVersion>) + Send>;

pub struct CommitSubmission {
	pub apply: CommitApply,
	pub completion: CommitCompletion,
}

pub type CommitBegin = Arc<dyn Fn() -> Result<CommandTransaction> + Send + Sync>;

#[derive(Clone)]
pub struct CommitHandle {
	begin: CommitBegin,
}

impl CommitHandle {
	pub fn new(begin: CommitBegin) -> Self {
		Self {
			begin,
		}
	}

	#[instrument(name = "transaction::commit::submit", level = "debug", skip_all)]
	pub fn submit(&self, submission: CommitSubmission) {
		let CommitSubmission {
			apply,
			completion,
		} = submission;
		(completion)(apply_and_commit(&self.begin, apply));
	}
}

fn apply_and_commit(begin: &CommitBegin, apply: CommitApply) -> Result<CommitVersion> {
	let mut transaction = (begin)()?;
	if let Err(e) = transaction.disable_conflict_tracking() {
		let _ = transaction.rollback();
		return Err(e);
	}
	if let Err(e) = (apply)(&mut transaction) {
		let _ = transaction.rollback();
		return Err(e);
	}
	transaction.commit_unchecked()
}
