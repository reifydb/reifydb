// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{VersionedGet, Versioned};
use reifydb_core::{EncodedKey, Version};

impl VersionedGet for Lmdb {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned> {
        todo!()
    }
}
