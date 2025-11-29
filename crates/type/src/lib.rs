// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod error;
mod fragment;
mod params;
pub mod util;
pub mod value;

pub use error::{Error, diagnostic};
pub use util::{BitVec, BitVecInner, BitVecIter, CowVec};

pub type Result<T> = std::result::Result<T, Error>;

pub use fragment::{
	BorrowedFragment, Fragment, IntoFragment, LazyFragment, OwnedFragment, StatementColumn, StatementLine,
};
pub use params::Params;
pub use value::{
	Blob, Constraint, Date, DateTime, Decimal, Duration, FromValueError, GetType, IdentityId, Int, IntoValue,
	OrderedF32, OrderedF64, RowNumber, Time, TryFromValue, TryFromValueCoerce, Type, TypeConstraint, Uint, Uuid4,
	Uuid7, Value, blob, boolean,
	boolean::parse_bool,
	decimal::parse_decimal,
	frame::{Frame, FrameColumn, FrameColumnData, FrameError, FrameRow, FrameRows, FromFrame, FromFrameError},
	is::{IsDate, IsFloat, IsInt, IsNumber, IsTemporal, IsTime, IsUint, IsUuid},
	number::{
		Promote, SafeAdd, SafeConvert, SafeDiv, SafeMul, SafeRemainder, SafeSub, parse_float,
		parse_primitive_int, parse_primitive_uint,
	},
	row_number::ROW_NUMBER_COLUMN_NAME,
	temporal,
	temporal::parse::{parse_date, parse_datetime, parse_duration, parse_time},
	uuid,
	uuid::{parse_uuid4, parse_uuid7},
};
