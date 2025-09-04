// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use encoded::{EncodedRow, EncodedRowIter, EncodedRowIterator};
pub use key::{EncodedKey, EncodedKeyRange};
pub use layout::{EncodedRowLayout, Field};
use reifydb_type::RowNumber;

mod bigdecimal;
mod bigint;
mod encoded;
mod get;
mod get_try;
mod key;
mod layout;
mod set;
mod value;

pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub layout: EncodedRowLayout,
}
