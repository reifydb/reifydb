// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::VersionedContains;
use crate::lmdb::Lmdb;
use reifydb_core::{EncodedKey, Version};
use crate::unversioned::UnversionedContains;

impl VersionedContains for Lmdb {
    fn contains(&self, _key: &EncodedKey, _version: Version) -> bool {
        todo!()
    }
}

impl UnversionedContains for Lmdb{
    fn contains_unversioned(&self, _key: &EncodedKey) -> bool {
        todo!()
    }
}