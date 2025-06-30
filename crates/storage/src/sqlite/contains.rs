// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::interface::UnversionedContains;
use reifydb_core::{EncodedKey, Error};

impl UnversionedContains for Sqlite {
    fn contains(&self, _key: &EncodedKey) -> Result<bool, Error> {
        todo!()
    }
}
