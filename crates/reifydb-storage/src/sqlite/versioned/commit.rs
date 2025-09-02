// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	CowVec, Result, Version,
	delta::Delta,
	interface::{TransactionId, VersionedCommit},
	return_internal_error,
};

use crate::sqlite::Sqlite;

impl VersionedCommit for Sqlite {
	fn commit(
		&self,
		delta: CowVec<Delta>,
		version: Version,
		transaction: TransactionId,
	) -> Result<()> {
		match self.execute_versioned_commit(delta, version, transaction)
		{
			Ok(()) => Ok(()),
			Err(e) => return_internal_error!(
				"Versioned commit failed: {}",
				e
			),
		}
	}
}
