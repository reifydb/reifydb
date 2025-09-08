// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use encoded::{
	EncodedIndexKey, EncodedIndexKeyIter, EncodedIndexKeyIterator,
};
pub use layout::{EncodedIndexLayout, IndexField};
pub use range::EncodedIndexKeyRange;

mod encoded;
mod get;
mod layout;
mod range;
mod set;
