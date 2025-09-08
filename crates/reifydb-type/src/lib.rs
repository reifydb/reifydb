// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod error;
mod fragment;
mod params;
pub mod util;
pub mod value;

pub use error::{Error, diagnostic};

pub type Result<T> = std::result::Result<T, Error>;

pub use fragment::{
	BorrowedFragment, Fragment, IntoFragment, LazyFragment, OwnedFragment,
	StatementColumn, StatementLine,
};
pub use params::Params;
pub use value::{
	Blob,
	Date,
	DateTime,
	GetType,
	IdentityId,
	Interval,
	IntoValue,
	OrderedF32,
	OrderedF64,
	RowNumber,
	Time,
	Type,

	// Parse functions
	Uuid4,
	// Traits
	Uuid7,

	// Number traits and operations
	Value,
	// Constants
	boolean::parse_bool,
	is::{
		IsDate, IsFloat, IsInt, IsNumber, IsTemporal, IsTime, IsUint,
		IsUuid,
	},
	number::{
		Promote, SafeAdd, SafeConvert, SafeDemote, SafeDiv, SafeMul,
		SafePromote, SafeRemainder, SafeSub, parse_float, parse_int,
		parse_uint,
	},
	// Core types
	row_number::ROW_NUMBER_COLUMN_NAME,
	temporal::parse::{
		parse_date, parse_datetime, parse_interval, parse_time,
	},
	uuid::{parse_uuid4, parse_uuid7},
};
pub use value::{blob, boolean, temporal, uuid};
