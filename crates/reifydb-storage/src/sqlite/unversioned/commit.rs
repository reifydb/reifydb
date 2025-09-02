// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result, delta::Delta, interface::UnversionedCommit,
	return_internal_error,
};

use crate::sqlite::Sqlite;

impl UnversionedCommit for Sqlite {
	fn commit(&mut self, delta: CowVec<Delta>) -> Result<()> {
		match self.execute_transaction(delta) {
			Ok(()) => Ok(()),
			Err(e) => return_internal_error!(
				"Database write failed: {}",
				e
			),
		}
	}
}
