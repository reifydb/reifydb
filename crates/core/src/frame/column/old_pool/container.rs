// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::IsNumber;
use crate::{BitVec, CowVec};

pub trait Container<T>: Default {
    /// Check if the buffer is empty.
    fn is_empty(&self) -> bool;

    /// Get the capacity of the buffer.
    fn capacity(&self) -> usize;

    /// Push a value to the buffer.
    fn push(&mut self, value: T);
}



