// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Value, encoding};
use crate::encoding::bincode::deserialize;

/// A row of values.
pub type Row = Vec<Value>;

pub fn deserialize_row(data: &[u8]) -> crate::encoding::Result<Row> {
	deserialize(data)
}

// impl encoding::Value for Row {}

/// A boxed row iterator.
pub type RowIter = Box<dyn RowIterator>;

pub trait RowIterator: Iterator<Item = Row> {}

impl<I: Iterator<Item = Row>> RowIterator for I {}
