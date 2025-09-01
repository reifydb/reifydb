// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod error;
mod fragment;
mod frame;
mod util;
mod value;

pub use error::diagnostic;
pub use error::Error;
pub use fragment::{
	BorrowedFragment, Fragment, IntoFragment, OwnedFragment,
	StatementColumn, StatementLine,
};
pub use value::{
	Blob, Date, DateTime, GetType, IdentityId, Interval, IntoValue,
	OrderedF32, OrderedF64, RowNumber, Time, Type, Uuid4, Uuid7, Value,
};
