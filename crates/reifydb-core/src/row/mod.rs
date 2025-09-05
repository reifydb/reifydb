// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use encoded::{EncodedRow, EncodedRowIter, EncodedRowIterator};
pub use key::{EncodedKey, EncodedKeyRange};
pub use layout::{EncodedRowLayout, Field};
use reifydb_type::RowNumber;

mod blob;
mod boolean;
mod date;
mod datetime;
mod decimal;
mod encoded;
mod f32;
mod f64;
mod i128;
mod i16;
mod i32;
mod i64;
mod i8;
mod identity_id;
mod interval;
mod key;
mod layout;
mod time;
mod u128;
mod u16;
mod u32;
mod u64;
mod u8;
mod undefined;
mod utf8;
mod uuid4;
mod uuid7;
mod value;
mod varint;
mod varuint;

pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub layout: EncodedRowLayout,
}
