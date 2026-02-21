// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cell::RefCell;

use crate::util::leb128::{Leb128, Leb128Error};

#[derive(Debug, PartialEq)]
pub enum Error {
	OutOfBounds,
	UnexpectedEndOfFile,
	InvalidLEB128Encoding,
}

impl From<Leb128Error> for Error {
	fn from(value: Leb128Error) -> Self {
		match value {
			Leb128Error::InvalidEncoding => Error::InvalidLEB128Encoding,
			Leb128Error::IncompleteEncoding => Error::UnexpectedEndOfFile,
		}
	}
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A `ByteReader` provides a simple mechanism for reading bytes sequentially
/// from a data source in memory. This struct is useful for scenarios where you
/// need to manually manage the position in a byte stream, such as when implementing
/// custom binary file parsers or decoding structured data from a raw byte buffer.
///
/// The `ByteReader` is generic over any data source that implements the `AsRef<[u8]>`
/// trait, which allows it to work with a variety of data types, including `Vec<u8>` and
/// byte slices (`&[u8]`). Internally, it keeps track of the current read position and
/// provides methods to read individual bytes and other primitive types from the data.
///
/// # Example
///
/// ```ignore
/// use crate::util::byte_reader::ByteReader;
/// let data: [u8;3] = [0x01, 0x02, 0x03];
/// let mut reader = ByteReader::new(data.as_ref());
///
/// assert_eq!(reader.read_u8().unwrap(), 0x01);
/// assert_eq!(reader.read_u8().unwrap(), 0x02);
/// assert_eq!(reader.read_u8().unwrap(), 0x03);
/// assert!(reader.read_u8().is_err()); // Out of bounds
/// ```
///
/// This struct does not perform any I/O operations and is designed to work entirely
/// with in-memory data.
pub struct ByteReader<'a> {
	data: &'a [u8],
	pos: RefCell<usize>,
}

impl<'a> ByteReader<'a> {
	/// Returns the total length of the data being read.
	///
	/// # Returns
	///
	/// A `usize` representing the total number of bytes in the data.
	fn length(&self) -> usize {
		self.data.as_ref().len()
	}

	/// Returns the current position within the data.
	///
	/// # Returns
	///
	/// A `usize` representing the current position (in bytes) of the reader.
	///
	/// This function returns the current index in the data where the next read operation will occur.
	pub fn pos(&self) -> usize {
		*self.pos.borrow()
	}

	/// Creates a new `ByteReader` from a given data source that implements `AsRef<[u8]>`.
	///
	/// # Arguments
	///
	/// * `data` - The data source, typically a `Vec<u8>` or a slice (`&[u8]`).
	///
	/// # Returns
	///
	/// A new `ByteReader` state.
	pub fn new(data: &'a [u8]) -> Self {
		ByteReader {
			data,
			pos: RefCell::new(0),
		}
	}

	/// Reads a single byte (`u8`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `u8` value, or a `ParseError` if the read fails.
	pub fn read_u8(&self) -> Result<u8> {
		let mut pos = self.pos.borrow_mut();

		if *pos + 1 > self.length() {
			return Err(Error::UnexpectedEndOfFile);
		}

		let res = self.data.as_ref()[*pos];
		*pos += 1;
		Ok(res)
	}

	/// Peeks at the next `u8` byte in the data stream without advancing the position.
	///
	/// This function allows you to inspect the byte at the current position in the data stream
	/// without moving the internal cursor forward. It can be useful when you need to make decisions
	/// based on the next byte without consuming it.
	///
	/// # Returns
	///
	/// - `Ok(u8)`: Returns the next byte in the data stream.
	/// - `Err(UnexpectedEndOfFile)`: Returns an error if the end of the data stream is reached.
	///
	/// # Errors
	///
	/// This function returns an `UnexpectedEndOfFile` error if there is no more data to read,
	/// i.e., if the current position is at or beyond the end of the data stream.
	///
	/// # Examples
	///
	/// ```ignore
	/// use crate::util::byte_reader::ByteReader;
	/// let reader = ByteReader::new(&[0x01, 0x02, 0x03]);
	/// assert_eq!(reader.peek_u8().unwrap(), 0x01);
	/// ```
	pub fn peek_u8(&self) -> Result<u8> {
		let pos = self.pos.borrow();

		if *pos + 1 > self.length() {
			return Err(Error::UnexpectedEndOfFile);
		}

		Ok(self.data.as_ref()[*pos])
	}

	/// Reads a 16-bit unsigned integer (`u16`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `u16` value, or a `ParseError` if the read fails.
	pub fn read_u16(&self) -> Result<u16> {
		let mut pos = self.pos.borrow_mut();

		if *pos + 2 > self.length() {
			return Err(Error::UnexpectedEndOfFile);
		}
		let _1 = self.data.as_ref()[*pos] as u16;
		let _2 = self.data.as_ref()[*pos + 1] as u16;
		let res = (_2 << 8) | _1;
		*pos += 2;
		Ok(res)
	}

	/// Reads a 32-bit unsigned integer (`u32`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `u32` value, or a `ParseError` if the read fails.
	pub fn read_u32(&self) -> Result<u32> {
		let mut pos = self.pos.borrow_mut();

		if *pos + 4 > self.length() {
			return Err(Error::UnexpectedEndOfFile);
		}

		let _1 = self.data.as_ref()[*pos] as u32;
		let _2 = self.data.as_ref()[*pos + 1] as u32;
		let _3 = self.data.as_ref()[*pos + 2] as u32;
		let _4 = self.data.as_ref()[*pos + 3] as u32;

		let res = _4 << 24 | _3 << 16 | _2 << 8 | _1;
		*pos += 4;
		Ok(res)
	}

	/// Reads a 32-bit float (`f32`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `f32` value, or a `ParseError` if the read fails.
	pub fn read_f32(&self) -> Result<f32> {
		let result = self.read_u32()?;
		Ok(f32::from_bits(result))
	}

	/// Reads a `u32` value encoded in LEB128 format from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the decoded `u32` value, or a `ParseError` if the read fails.
	pub fn read_leb128_u32(&self) -> Result<u32> {
		let (result, consumed) = u32::read_leb128(self.peek_range(5)?)?;
		let mut pos = self.pos.borrow_mut();
		*pos += consumed;
		Ok(result)
	}

	/// Reads a `u64` value encoded in LEB128 format from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the decoded `u64` value, or a `ParseError` if the read fails.
	pub fn read_leb128_u64(&self) -> Result<u64> {
		let (result, consumed) = u64::read_leb128(self.peek_range(10)?)?;
		let mut pos = self.pos.borrow_mut();
		*pos += consumed;
		Ok(result)
	}

	/// Reads an `i32` value encoded in LEB128 format from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the decoded `i32` value, or a `ParseError` if the read fails.
	pub fn read_leb128_i32(&self) -> Result<i32> {
		let (result, consumed) = i32::read_leb128(self.peek_range(5)?)?;
		let mut pos = self.pos.borrow_mut();
		*pos += consumed;
		Ok(result)
	}

	/// Reads an `i64` value encoded in LEB128 format from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the decoded `i64` value, or a `ParseError` if the read fails.
	pub fn read_leb128_i64(&self) -> Result<i64> {
		let (result, consumed) = i64::read_leb128(self.peek_range(10)?)?;
		let mut pos = self.pos.borrow_mut();
		*pos += consumed;
		Ok(result)
	}

	/// Reads a 64-bit unsigned integer (`u64`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `u64` value, or a `ParseError` if the read fails.
	pub fn read_u64(&self) -> Result<u64> {
		let mut pos = self.pos.borrow_mut();

		if *pos + 8 > self.length() {
			return Err(Error::UnexpectedEndOfFile);
		}

		let _1 = self.data.as_ref()[*pos] as u64;
		let _2 = self.data.as_ref()[*pos + 1] as u64;
		let _3 = self.data.as_ref()[*pos + 2] as u64;
		let _4 = self.data.as_ref()[*pos + 3] as u64;
		let _5 = self.data.as_ref()[*pos + 4] as u64;
		let _6 = self.data.as_ref()[*pos + 5] as u64;
		let _7 = self.data.as_ref()[*pos + 6] as u64;
		let _8 = self.data.as_ref()[*pos + 7] as u64;

		let res = _8 << 56 | _7 << 48 | _6 << 40 | _5 << 32 | _4 << 24 | _3 << 16 | _2 << 8 | _1;
		*pos += 8;
		Ok(res)
	}

	/// Reads a 64-bit float (`f64`) from the current reader position.
	///
	/// # Returns
	///
	/// A `Result` containing the read `f64` value, or a `ParseError` if the read fails.
	pub fn read_f64(&self) -> Result<f64> {
		let result = self.read_u64()?;
		Ok(f64::from_bits(result))
	}

	/// Reads a slice of bytes of a specified length from the current reader position.
	/// Advances the reader position by the length of the slice.
	///
	/// # Arguments
	///
	/// * `len` - The number of bytes to read.
	///
	/// # Returns
	///
	/// A `Result` containing the slice of bytes read, or a `ParseError` if there is not
	/// enough data left to read the requested number of bytes.
	pub fn read_range(&self, len: usize) -> Result<Box<[u8]>> {
		let mut pos = self.pos.borrow_mut();

		let data = self.data.as_ref();

		if *pos + len > data.len() {
			return Err(Error::UnexpectedEndOfFile);
		}

		let result = &data[*pos..*pos + len];
		*pos += len;

		Ok(Box::from(result))
	}

	/// Peeks at a range of bytes starting from the current position without advancing the position.
	///
	/// This function returns a slice of bytes from the current position up to `len` bytes long,
	/// but it will return fewer bytes if the end of the data is reached before `len` bytes are available.
	/// The position within the data is not modified by this operation.
	///
	/// # Parameters
	///
	/// - `len`: The number of bytes to peek at starting from the current position.
	///
	/// # Returns
	///
	/// - `Ok(&[u8])`: A slice containing the peeked bytes, or as many bytes as remain from the current position.
	/// - `Err`: Any error that might occur (e.g., if the `pos` or `data` fields are invalid or in an incorrect
	///   state).
	///
	/// # Example
	///
	/// ```ignore
	/// use crate::util::byte_reader::ByteReader;
	/// let data: Vec<u8> = vec![1, 2, 3, 4, 5];
	/// let reader = ByteReader::new(&data);
	/// assert_eq!(reader.peek_range(3).unwrap(), &[1, 2, 3]);
	/// assert_eq!(reader.peek_range(10).unwrap(), &[1, 2, 3, 4, 5]);
	/// ```
	pub fn peek_range(&self, len: usize) -> Result<&[u8]> {
		let pos = self.pos.borrow();
		let data = self.data.as_ref();
		let end_pos = (*pos + len).min(data.len());

		let result = &data[*pos..end_pos];
		Ok(result)
	}

	/// Seeks to a new position based on the provided offset.
	///
	/// # Arguments
	///
	/// * `offset` - The offset to move the reader by. It can be positive (to move forward) or negative (to move
	///   backward).
	///
	/// # Returns
	///
	/// A `Result` containing the new position after applying the offset or a `ParseError`
	/// if the computed position is out of bounds.
	pub fn seek(&self, offset: isize) -> Result<usize> {
		let mut pos = self.pos.borrow_mut();
		let new_pos = if offset.is_negative() {
			// Ensure we do not go below 0
			pos.saturating_sub(offset.unsigned_abs())
		} else {
			// Ensure we do not go beyond the end of the data
			pos.saturating_add(offset as usize)
		};

		let data_len = self.length();

		if new_pos > data_len {
			Err(Error::OutOfBounds)
		} else {
			*pos = new_pos;
			Ok(*pos)
		}
	}

	/// Checks if the reader has reached the end of the file.
	///
	/// # Returns
	///
	/// A `bool` indicating whether the reader is at the end of the file.
	pub fn eof(&self) -> bool {
		*self.pos.borrow() >= self.length()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::util::byte_reader::Error::OutOfBounds;

	#[test]
	fn read_empty() {
		let data: &[u8] = &[];
		let ti = ByteReader::new(data);

		assert!(ti.read_u8().is_err());
		assert!(ti.read_u16().is_err());
		assert!(ti.read_u32().is_err());
		assert!(ti.read_u64().is_err());
	}

	#[test]
	fn read_u8() {
		let data: &[u8] = &[0x05, 0x06, 0x07, 0x08];
		let ti = ByteReader::new(data);

		assert_eq!(ti.read_u8().unwrap(), 0x05);
		assert_eq!(ti.read_u8().unwrap(), 0x06);
		assert_eq!(ti.read_u8().unwrap(), 0x07);
		assert_eq!(ti.read_u8().unwrap(), 0x08);
	}

	#[test]
	fn read_u16() {
		let data: &[u8] = &[0x05, 0x06, 0x07, 0x08];
		let ti = ByteReader::new(data);

		assert_eq!(ti.read_u16().unwrap(), 0x0605); // Little-endian: 0x0506
		assert_eq!(ti.read_u16().unwrap(), 0x0807); // Little-endian: 0x0708
	}

	#[test]
	fn read_u32() {
		let data: &[u8] = &[0x05, 0x06, 0x07, 0x08];
		let ti = ByteReader::new(data);

		assert_eq!(ti.read_u32().unwrap(), 0x08070605); // Little-endian: 0x05060708
	}

	#[test]
	fn read_f32() {
		let cases = [
			// Little-endian bytes for 1.0f32
			(vec![0x00, 0x00, 0x80, 0x3F], 1.0f32),
			// Little-endian bytes for -1.0f32
			(vec![0x00, 0x00, 0x80, 0xBF], -1.0f32),
			// Little-endian bytes for 0.0f32
			(vec![0x00, 0x00, 0x00, 0x00], 0.0f32),
			// Little-endian bytes for -0.0f32
			(vec![0x00, 0x00, 0x00, 0x80], -0.0f32),
			// Little-endian bytes for 2.5f32
			(vec![0x00, 0x00, 0x20, 0x40], 2.5f32),
			// Little-endian bytes for -2.5f32
			(vec![0x00, 0x00, 0x20, 0xC0], -2.5f32),
		];

		for (data, expected) in cases.iter() {
			let ti = ByteReader::new(data);
			assert_eq!(ti.read_f32().unwrap(), *expected);
		}
	}

	#[test]
	fn read_u64() {
		let data: &[u8] = &[0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10];
		let ti = ByteReader::new(data);

		assert_eq!(ti.read_u64().unwrap(), 0x100F0E0D0C0B0A09); // Little-endian: 0x090A0B0C0D0E0F10
	}

	#[test]
	fn read_f64() {
		let cases = [
			// Little-endian bytes for 1.0f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F], 1.0f64),
			// Little-endian bytes for -1.0f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0xBF], -1.0f64),
			// Little-endian bytes for 0.0f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 0.0f64),
			// Little-endian bytes for -0.0f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80], -0.0f64),
			// Little-endian bytes for 2.5f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40], 2.5f64),
			// Little-endian bytes for -2.5f64
			(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xC0], -2.5f64),
		];

		for (data, expected) in cases.iter() {
			let ti = ByteReader::new(data);

			assert_eq!(ti.read_f64().unwrap(), *expected);
		}
	}

	#[test]
	fn read_range() {
		let data: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
		let ti = ByteReader::new(data);

		assert_eq!(ti.read_range(4).unwrap().as_ref(), [0x01, 0x02, 0x03, 0x04]);
		assert_eq!(ti.read_u8().unwrap(), 0x05);
		assert_eq!(ti.read_range(2).unwrap().as_ref(), [0x06, 0x07]);
		assert_eq!(ti.read_u8().unwrap(), 0x08);
	}

	#[test]
	fn read_range_out_of_bounds() {
		let data: &[u8] = &[0x01, 0x02, 0x03, 0x04];
		let ti = ByteReader::new(data);

		ti.seek(3).unwrap();
		assert!(ti.read_range(2).is_err());
	}

	#[test]
	fn seek() {
		let data = b"Hello, world!";
		let reader = ByteReader::new(&data[..]);

		// Test seeking forward within bounds
		assert_eq!(reader.seek(7).unwrap(), 7);
		assert_eq!(reader.seek(3).unwrap(), 10);

		// Test seeking backward within bounds
		assert_eq!(reader.seek(-5).unwrap(), 5);
		assert_eq!(reader.seek(-10).unwrap(), 0); // Should clamp to 0

		// Test seeking beyond the data length
		assert_eq!(reader.seek(50).err().unwrap(), OutOfBounds);
	}

	#[test]
	fn read_leb128_u32_single_byte() {
		let data = [0x7F]; // 127 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u32().unwrap();
		assert_eq!(result, 127);
	}

	#[test]
	fn read_leb128_u32_multiple_bytes() {
		let data = [0xE5, 0x8E, 0x26]; // 624485 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u32().unwrap();
		assert_eq!(result, 624485);
	}

	#[test]
	fn read_leb128_u32_max_u32() {
		let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]; // Maximum u32 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u32().unwrap();
		assert_eq!(result, 4294967295); // Max u32 value
	}

	#[test]
	fn read_leb128_u32_unexpected_eof() {
		let data = [0x80]; // Incomplete LEB128 encoding
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u32();
		assert!(matches!(result, Err(Error::UnexpectedEndOfFile)));
	}

	#[test]
	fn read_leb128_u32_invalid_encoding() {
		let data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF]; // Too many bytes for a valid u32
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u32();
		assert!(matches!(result, Err(Error::InvalidLEB128Encoding)));
	}

	#[test]
	fn eof_empty_data() {
		let ti = ByteReader::new(&[]);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_positive_single_byte() {
		let data = [0x3F]; // 63 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, 63);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_negative_single_byte() {
		let data = [0x41]; // -63 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, -63);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_positive_multiple_bytes() {
		let data = [0xE5, 0x8E, 0x26]; // 624485 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, 624485);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_negative_multiple_bytes() {
		let data = [0x9B, 0xF1, 0x59]; // -624485 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, -624485);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_max_i32() {
		let data = [0xFF, 0xFF, 0xFF, 0xFF, 0x07]; // Maximum i32 in LEB128 (2147483647)
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, i32::MAX); // Max i32 value
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_min_i32() {
		let data = [0x80, 0x80, 0x80, 0x80, 0x78]; // Minimum i32 in LEB128 (-2147483648)
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32().unwrap();
		assert_eq!(result, i32::MIN);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i32_unexpected_eof() {
		let data = [0x80]; // Incomplete LEB128 encoding
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32();
		assert!(matches!(result, Err(Error::UnexpectedEndOfFile)));
	}

	#[test]
	fn read_leb128_i32_invalid_encoding() {
		let data = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF]; // Too many bytes for a valid i32
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i32();
		assert!(matches!(result, Err(Error::InvalidLEB128Encoding)));
	}

	#[test]
	fn read_leb128_i64_max_i64() {
		let data = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0]; // Maximum i64 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i64().unwrap();
		assert_eq!(result, i64::MAX);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_i64_min_i64() {
		let data = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]; // Minimum i64 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_i64().unwrap();
		assert_eq!(result, i64::MIN);
		assert!(ti.eof());
	}

	#[test]
	fn read_leb128_u64_max_u64() {
		let data = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]; // Maximum u64 in LEB128
		let ti = ByteReader::new(&data);
		let result = ti.read_leb128_u64().unwrap();
		assert_eq!(result, u64::MAX);
		assert!(ti.eof());
	}

	#[test]
	fn peek_range_within_bounds() {
		let given = [1, 2, 3, 4, 5];
		let ti = ByteReader::new(&given);
		let result = ti.peek_range(3).unwrap();
		assert_eq!(result, &[1, 2, 3]);
	}

	#[test]
	fn peek_range_past_end() {
		let given = [1, 2, 3, 4, 5];
		let ti = ByteReader::new(&given);
		let result = ti.peek_range(10).unwrap();
		assert_eq!(result, &[1, 2, 3, 4, 5]);
	}

	#[test]
	fn peek_range_empty_data() {
		let given = [];
		let ti = ByteReader::new(&given);
		let result = ti.peek_range(5).unwrap();
		assert_eq!(result, &[]);
	}

	#[test]
	fn peek_range_zero_len() {
		let given = [1, 2, 3, 4, 5];
		let ti = ByteReader::new(&given);
		let result = ti.peek_range(0).unwrap();
		assert_eq!(result, &[]);
	}

	#[test]
	fn peek_range_after_advancing_pos() {
		let given = [1, 2, 3, 4, 5];
		let ti = ByteReader::new(&given);
		*ti.pos.borrow_mut() = 2;
		let result = ti.peek_range(2).unwrap();
		assert_eq!(result, &[3, 4]);
	}

	#[test]
	fn peek_u8_at_start() {
		let ti = ByteReader::new(&[0x01, 0x02, 0x03]);
		assert_eq!(ti.peek_u8().unwrap(), 0x01);
		assert_eq!(ti.peek_u8().unwrap(), 0x01); // Position should not advance
	}

	#[test]
	fn peek_u8_past_end() {
		let ti = ByteReader::new(&[0x01, 0x02, 0x03]);
		ti.read_range(3).unwrap(); // Advance past the last byte
		assert!(ti.peek_u8().is_err());
	}

	#[test]
	fn peek_u8_empty_data() {
		let ti = ByteReader::new(&[]);
		assert!(ti.peek_u8().is_err()); // Should return an error
	}
}
