// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
use crate::encoding::bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// A boxed row iterator.
pub type RowIter = Box<dyn RowIterator>;

pub trait RowIterator: Iterator<Item = EncodedRow> {}

impl<I: Iterator<Item = EncodedRow>> RowIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize)]
// validity:values
#[derive(PartialEq, Eq)]
pub struct EncodedRow(pub AsyncCowVec<u8>);

impl Deref for EncodedRow {
    type Target = AsyncCowVec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl EncodedRow {
    pub fn make_mut(&mut self) -> &mut [u8] {
        self.0.make_mut()
    }

    #[inline]
    pub fn is_defined(&self, index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        (self.0[byte] & (1 << bit)) != 0
    }

    pub(crate) fn set_valid(&mut self, index: usize, valid: bool) {
        let byte = index / 8;
        let bit = index % 8;
        if valid {
            self.0.make_mut()[byte] |= 1 << bit;
        } else {
            self.0.make_mut()[byte] &= !(1 << bit);
        }
    }
}

#[deprecated]
/// FIXME remove me
pub fn deprecated_deserialize_row(data: &[u8]) -> crate::encoding::Result<EncodedRow> {
    deserialize(data)
}

#[deprecated]
/// FIXME remove me
pub fn deprecated_serialize_row(row: &EncodedRow) -> crate::encoding::Result<Vec<u8>> {
    Ok(serialize(row))
}

#[cfg(test)]
mod tests {}
