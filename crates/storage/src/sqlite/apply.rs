// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::AsyncCowVec;
use reifydb_core::delta::Delta;
use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedApply;

impl UnversionedApply for Sqlite{
	fn apply(&self, delta: AsyncCowVec<Delta>) {
		todo!()
	}
}