// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use reifydb_core::interface::{VersionedContains, VersionedGet};
use reifydb_core::{EncodedKey, Version};

impl VersionedContains for Sqlite {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool {
        // FIXME this can be done better than this
        self.get(key, version).is_some()
    }
}