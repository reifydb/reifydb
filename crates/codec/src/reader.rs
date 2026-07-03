// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::error::DecodeError;

pub struct Reader<'a> {
	bytes: &'a [u8],
	pos: usize,
}

impl<'a> Reader<'a> {
	pub fn new(bytes: &'a [u8]) -> Self {
		Self {
			bytes,
			pos: 0,
		}
	}

	pub fn position(&self) -> usize {
		self.pos
	}

	pub fn remaining(&self) -> usize {
		self.bytes.len() - self.pos
	}

	pub fn is_empty(&self) -> bool {
		self.remaining() == 0
	}

	pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
		if self.remaining() < n {
			return Err(DecodeError::UnexpectedEof {
				expected: n,
				available: self.remaining(),
			});
		}
		let slice = &self.bytes[self.pos..self.pos + n];
		self.pos += n;
		Ok(slice)
	}

	pub fn u8(&mut self) -> Result<u8, DecodeError> {
		Ok(self.take(1)?[0])
	}

	pub fn u16(&mut self) -> Result<u16, DecodeError> {
		Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
	}

	pub fn u32(&mut self) -> Result<u32, DecodeError> {
		Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
	}

	pub fn u64(&mut self) -> Result<u64, DecodeError> {
		Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
	}

	pub fn u128(&mut self) -> Result<u128, DecodeError> {
		Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
	}

	pub fn i8(&mut self) -> Result<i8, DecodeError> {
		Ok(self.take(1)?[0] as i8)
	}

	pub fn i16(&mut self) -> Result<i16, DecodeError> {
		Ok(i16::from_le_bytes(self.take(2)?.try_into().unwrap()))
	}

	pub fn i32(&mut self) -> Result<i32, DecodeError> {
		Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
	}

	pub fn i64(&mut self) -> Result<i64, DecodeError> {
		Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
	}

	pub fn i128(&mut self) -> Result<i128, DecodeError> {
		Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
	}

	pub fn f32(&mut self) -> Result<f32, DecodeError> {
		Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
	}

	pub fn f64(&mut self) -> Result<f64, DecodeError> {
		Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
	}
}
