// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;
use crate::encoding::bincode::{deserialize, serialize};

/// A row of values.
pub type Row = Vec<Value>;

pub fn deserialize_row(data: &[u8]) -> crate::encoding::Result<Row> {
    deserialize(data)
}

pub fn serialize_row(row: &Row) -> crate::encoding::Result<Vec<u8>> {
    Ok(serialize(row))
}

/// A boxed row iterator.
pub type RowIter = Box<dyn RowIterator>;

pub trait RowIterator: Iterator<Item = Row> {}

impl<I: Iterator<Item = Row>> RowIterator for I {}
