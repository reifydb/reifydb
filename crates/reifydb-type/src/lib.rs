// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

mod error;
mod fragment;
pub mod util;
pub mod value;

pub use error::{Error, diagnostic};

pub type Result<T> = std::result::Result<T, Error>;

pub use fragment::{
	BorrowedFragment, Fragment, IntoFragment, LazyFragment, OwnedFragment,
	StatementColumn, StatementLine,
};
pub use value::{
	Blob,
	Date,
	DateTime,
	Decimal,
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

	VarInt,
	VarUint,
	// Parse functions
	boolean::parse_bool,
	decimal::parse_decimal,
	is::{
		IsDate, IsFloat, IsInt, IsNumber, IsTemporal, IsTime, IsUint,
		IsUuid,
	},
	number::{
		Promote, SafeAdd, SafeConvert, SafeDemote, SafeDiv, SafeMul,
		SafePromote, SafeRemainder, SafeSub, parse_float, parse_int,
		parse_uint,
	},
	row_number::ROW_NUMBER_COLUMN_NAME,
	temporal::parse::{
		parse_date, parse_datetime, parse_interval, parse_time,
	},
	uuid::{parse_uuid4, parse_uuid7},
	varint::parse_varint,
	varuint::parse_varuint,
};
pub use value::{blob, boolean, temporal, uuid};
