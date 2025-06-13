// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Rx;
use reifydb_core::EncodedKey;
use reifydb_core::row::EncodedRow;

pub trait Tx: Rx {
    fn set(&mut self, key: EncodedKey, row: EncodedRow) -> crate::Result<()>;
    fn commit(self) -> crate::Result<()>;
    fn rollback(self) -> crate::Result<()>;
}
