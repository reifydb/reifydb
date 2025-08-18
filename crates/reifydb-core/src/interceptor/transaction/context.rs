// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Version,
	interface::{CommandTransaction, Transaction},
};

/// Context for pre-commit interceptors
pub struct PreCommitContext<'a, T: Transaction> {
	pub txn: &'a mut CommandTransaction<T>,
}

impl<'a, T: Transaction> PreCommitContext<'a, T> {
	pub fn new(txn: &'a mut CommandTransaction<T>) -> Self {
		Self {
			txn,
		}
	}
}

/// Context for post-commit interceptors
pub struct PostCommitContext {
	pub version: Version,
}

impl PostCommitContext {
	pub fn new(version: Version) -> Self {
		Self {
			version,
		}
	}
}
