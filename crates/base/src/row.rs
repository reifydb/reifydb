// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Value;
use dyn_clone::DynClone;

/// A row of values.
pub type Row = Vec<Value>;

/// A boxed row iterator.
pub type RowIter = Box<dyn RowIterator>;

/// A row iterator trait, which requires the iterator to be clonable and
/// object-safe. Cloning enables us to reset an iterator back to an initial state,
pub trait RowIterator: Iterator<Item = Row> + DynClone {}

dyn_clone::clone_trait_object!(RowIterator);

impl<I: Iterator<Item = Row> + DynClone> RowIterator for I {}
