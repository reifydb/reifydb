// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedApply;
use reifydb_core::AsyncCowVec;
use reifydb_core::delta::Delta;

impl UnversionedApply for Sqlite {
    fn apply_unversioned(&mut self, delta: AsyncCowVec<Delta>) {
        todo!()
    }
}
