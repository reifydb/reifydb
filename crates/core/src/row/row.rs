// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::AsyncCowVec;
use crate::encoding::bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// A boxed row iterator.
pub type RowIter = Box<dyn RowIterator>;

pub trait RowIterator: Iterator<Item = Row> {}

impl<I: Iterator<Item = Row>> RowIterator for I {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row(pub AsyncCowVec<u8>);

impl Deref for Row {
    type Target = AsyncCowVec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Row {
    pub fn make_mut(&mut self) -> &mut [u8] {
        self.0.make_mut()
    }
}

#[deprecated]
pub fn deprecated_deserialize_row(data: &[u8]) -> crate::encoding::Result<Row> {
    deserialize(data)
}

#[deprecated]
pub fn deprecated_serialize_row(row: &Row) -> crate::encoding::Result<Vec<u8>> {
    Ok(serialize(row))
}
