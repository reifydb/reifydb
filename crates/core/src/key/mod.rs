// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
pub use range::EncodedKeyRange;
use std::ops::Deref;

mod range;

#[derive(Debug, Clone, PartialOrd, Ord, Hash, PartialEq, Eq)]
pub struct EncodedKey(pub AsyncCowVec<u8>);

impl Deref for EncodedKey {
    type Target = AsyncCowVec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EncodedKey {
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self(AsyncCowVec::new(key.into()))
    }
}
