// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::interface::{UnversionedContains, UnversionedGet};
use reifydb_core::{EncodedKey, Error};

impl UnversionedContains for Sqlite {
    fn contains(&self, key: &EncodedKey) -> Result<bool, Error> {
        self.get(key).map(|result| result.is_some())
    }
}
