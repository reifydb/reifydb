// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Version};

pub struct Stored {
    pub key: EncodedKey,
    pub row: EncodedRow,
    pub version: Version,
}
