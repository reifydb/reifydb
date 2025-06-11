// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::row::Row;
use reifydb_core::{Key, Version};

pub struct Stored {
    pub key: Key,
    pub row: Row,
    pub version: Version,
}
