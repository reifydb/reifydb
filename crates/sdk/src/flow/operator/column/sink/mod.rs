// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod in_process;

use reifydb_value::value::{date::Date, datetime::DateTime, decimal::Decimal, duration::Duration, time::Time};

use crate::error::SdkError;

pub trait RowSink {
	fn push_u8(&mut self, col: usize, v: u8) -> Result<(), SdkError>;
	fn push_u16(&mut self, col: usize, v: u16) -> Result<(), SdkError>;
	fn push_u32(&mut self, col: usize, v: u32) -> Result<(), SdkError>;
	fn push_u64(&mut self, col: usize, v: u64) -> Result<(), SdkError>;
	fn push_u128(&mut self, col: usize, v: u128) -> Result<(), SdkError>;
	fn push_i8(&mut self, col: usize, v: i8) -> Result<(), SdkError>;
	fn push_i16(&mut self, col: usize, v: i16) -> Result<(), SdkError>;
	fn push_i32(&mut self, col: usize, v: i32) -> Result<(), SdkError>;
	fn push_i64(&mut self, col: usize, v: i64) -> Result<(), SdkError>;
	fn push_i128(&mut self, col: usize, v: i128) -> Result<(), SdkError>;
	fn push_f32(&mut self, col: usize, v: f32) -> Result<(), SdkError>;
	fn push_f64(&mut self, col: usize, v: f64) -> Result<(), SdkError>;
	fn push_date(&mut self, col: usize, v: Date) -> Result<(), SdkError>;
	fn push_datetime(&mut self, col: usize, v: DateTime) -> Result<(), SdkError>;
	fn push_time(&mut self, col: usize, v: Time) -> Result<(), SdkError>;
	fn push_duration(&mut self, col: usize, v: Duration) -> Result<(), SdkError>;
	fn push_bool(&mut self, col: usize, v: bool) -> Result<(), SdkError>;
	fn push_utf8(&mut self, col: usize, v: &str) -> Result<(), SdkError>;
	fn push_blob(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError>;
	fn push_decimal(&mut self, col: usize, v: &Decimal) -> Result<(), SdkError>;
	fn push_none(&mut self, col: usize) -> Result<(), SdkError>;
}
