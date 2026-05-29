// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod ffi;
pub mod native;

use reifydb_value::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

use crate::error::SdkError;

pub trait RowSink {
	fn push_u8(&mut self, col: usize, v: u8);
	fn push_u16(&mut self, col: usize, v: u16);
	fn push_u32(&mut self, col: usize, v: u32);
	fn push_u64(&mut self, col: usize, v: u64);
	fn push_u128(&mut self, col: usize, v: u128);
	fn push_i8(&mut self, col: usize, v: i8);
	fn push_i16(&mut self, col: usize, v: i16);
	fn push_i32(&mut self, col: usize, v: i32);
	fn push_i64(&mut self, col: usize, v: i64);
	fn push_i128(&mut self, col: usize, v: i128);
	fn push_f32(&mut self, col: usize, v: f32);
	fn push_f64(&mut self, col: usize, v: f64);
	fn push_date(&mut self, col: usize, v: Date);
	fn push_datetime(&mut self, col: usize, v: DateTime);
	fn push_time(&mut self, col: usize, v: Time);
	fn push_duration(&mut self, col: usize, v: Duration);
	fn push_bool(&mut self, col: usize, v: bool);
	fn push_utf8(&mut self, col: usize, v: &str) -> Result<(), SdkError>;
	fn push_blob(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError>;
	fn push_decimal_bytes(&mut self, col: usize, v: &[u8]) -> Result<(), SdkError>;
	fn push_none(&mut self, col: usize) -> Result<(), SdkError>;
}
