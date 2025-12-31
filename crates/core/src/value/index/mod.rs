// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub use encoded::{EncodedIndexKey, EncodedIndexKeyIter, EncodedIndexKeyIterator};
pub use layout::{EncodedIndexLayout, IndexField};
pub use range::EncodedIndexKeyRange;

mod encoded;
mod get;
mod layout;
mod range;
mod set;
