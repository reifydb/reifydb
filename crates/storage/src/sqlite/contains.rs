// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedContains;
use reifydb_core::EncodedKey;

impl UnversionedContains for Sqlite {
    fn contains_unversioned(&self, _key: &EncodedKey) -> bool {
        todo!()
    }
}
