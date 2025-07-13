// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use reifydb_core::interface::{UnversionedContains, VersionedContains};
use reifydb_core::{EncodedKey, Error, Version};

impl VersionedContains for Lmdb {
    fn contains(&self, _key: &EncodedKey, _version: Version) -> bool {
        todo!()
    }
}

impl UnversionedContains for Lmdb {
    fn contains(&self, _key: &EncodedKey) -> Result<bool, Error> {
        todo!()
    }
}
