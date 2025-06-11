// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Get, Stored};
use reifydb_core::{EncodedKey, Version};

impl Get for Lmdb {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Stored> {
        todo!()
    }
}
