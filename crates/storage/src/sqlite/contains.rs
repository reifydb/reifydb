// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::EncodedKey;
use reifydb_core::interface::UnversionedContains;

impl UnversionedContains for Sqlite {
    fn contains_unversioned(&self, _key: &EncodedKey) -> bool {
        todo!()
    }
}
