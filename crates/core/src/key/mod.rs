// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::CowVec;
pub use range::EncodedKeyRange;
use std::ops::Deref;

mod range;

#[derive(Debug, Clone, PartialOrd, Ord, Hash, PartialEq, Eq)]
pub struct EncodedKey(pub CowVec<u8>);

impl Deref for EncodedKey {
    type Target = CowVec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EncodedKey {
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self(CowVec::new(key.into()))
    }
}
