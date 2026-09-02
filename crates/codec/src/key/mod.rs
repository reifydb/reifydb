// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_value::value::datetime::DateTime;

pub mod buf;
pub mod deserializer;
pub mod encoded;
pub mod serializer;
pub mod sort;
pub(crate) mod varint;

use std::{f32, f64};

use reifydb_value::{
	Result,
	error::{Error, TypeError},
};

use crate::key::buf::KeyBuf;

pub trait ByteSink {
	fn push(&mut self, byte: u8);
	fn extend_from_slice(&mut self, slice: &[u8]);
}

impl ByteSink for Vec<u8> {
	fn push(&mut self, byte: u8) {
		Vec::push(self, byte);
	}
	fn extend_from_slice(&mut self, slice: &[u8]) {
		Vec::extend_from_slice(self, slice);
	}
}

impl ByteSink for KeyBuf {
	fn push(&mut self, byte: u8) {
		KeyBuf::push(self, byte);
	}
	fn extend_from_slice(&mut self, slice: &[u8]) {
		KeyBuf::extend_from_slice(self, slice);
	}
}

pub fn encode_bool(value: bool) -> u8 {
	if value {
		0x00
	} else {
		0x01
	}
}

pub fn decode_bool(byte: u8) -> Result<bool> {
	match byte {
		0x00 => Ok(true),
		0x01 => Ok(false),
		b => Err(Error::from(TypeError::SerdeKeycode {
			message: format!("invalid boolean value {b}"),
		})),
	}
}

pub fn encode_f32(value: f32) -> [u8; 4] {
	let bits = value.to_bits();
	if value.is_sign_negative() {
		bits.to_be_bytes()
	} else {
		(!(bits ^ 0x80000000)).to_be_bytes()
	}
}

pub fn decode_f32(bytes: [u8; 4]) -> f32 {
	let encoded = u32::from_be_bytes(bytes);
	if encoded & 0x80000000 != 0 {
		f32::from_bits(encoded)
	} else {
		f32::from_bits(!encoded ^ 0x80000000)
	}
}

pub fn encode_f64(value: f64) -> [u8; 8] {
	let bits = value.to_bits();
	if value.is_sign_negative() {
		bits.to_be_bytes()
	} else {
		(!(bits ^ 0x8000000000000000)).to_be_bytes()
	}
}

pub fn decode_f64(bytes: [u8; 8]) -> f64 {
	let encoded = u64::from_be_bytes(bytes);
	if encoded & 0x8000000000000000 != 0 {
		f64::from_bits(encoded)
	} else {
		f64::from_bits(!encoded ^ 0x8000000000000000)
	}
}

pub fn encode_i8(value: i8) -> [u8; 1] {
	(!(value as u8 ^ 0x80)).to_be_bytes()
}

pub fn decode_i8(bytes: [u8; 1]) -> i8 {
	(!u8::from_be_bytes(bytes) ^ 0x80) as i8
}

pub fn encode_i16(value: i16) -> [u8; 2] {
	(!(value as u16 ^ 0x8000)).to_be_bytes()
}

pub fn decode_i16(bytes: [u8; 2]) -> i16 {
	(!u16::from_be_bytes(bytes) ^ 0x8000) as i16
}

pub fn encode_i32(value: i32) -> [u8; 4] {
	(!(value as u32 ^ 0x80000000)).to_be_bytes()
}

pub fn decode_i32(bytes: [u8; 4]) -> i32 {
	(!u32::from_be_bytes(bytes) ^ 0x80000000) as i32
}

pub fn encode_i64(value: i64) -> [u8; 8] {
	(!(value as u64 ^ 0x8000000000000000)).to_be_bytes()
}

pub fn decode_i64(bytes: [u8; 8]) -> i64 {
	(!u64::from_be_bytes(bytes) ^ 0x8000000000000000) as i64
}

pub fn encode_i128(value: i128) -> [u8; 16] {
	(!(value as u128 ^ 0x80000000000000000000000000000000)).to_be_bytes()
}

pub fn decode_i128(bytes: [u8; 16]) -> i128 {
	(!u128::from_be_bytes(bytes) ^ 0x80000000000000000000000000000000) as i128
}

pub fn encode_u8(value: u8) -> u8 {
	!value
}

pub fn decode_u8(byte: u8) -> u8 {
	!byte
}

pub fn encode_u16(value: u16) -> [u8; 2] {
	(!value).to_be_bytes()
}

pub fn decode_u16(bytes: [u8; 2]) -> u16 {
	!u16::from_be_bytes(bytes)
}

pub fn encode_u32(value: u32) -> [u8; 4] {
	(!value).to_be_bytes()
}

pub fn decode_u32(bytes: [u8; 4]) -> u32 {
	!u32::from_be_bytes(bytes)
}

pub fn encode_u64(value: u64) -> [u8; 8] {
	(!value).to_be_bytes()
}

pub fn decode_u64(bytes: [u8; 8]) -> u64 {
	!u64::from_be_bytes(bytes)
}

pub fn decode_u64_from(input: &mut &[u8]) -> Result<u64> {
	if input.len() < 8 {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding u64".to_string(),
		}));
	}
	let value = decode_u64(input[..8].try_into()?);
	*input = &input[8..];
	Ok(value)
}

pub fn encode_u64_asc(value: u64) -> [u8; 8] {
	value.to_be_bytes()
}

pub fn decode_u64_asc(bytes: [u8; 8]) -> u64 {
	u64::from_be_bytes(bytes)
}

pub fn encode_datetime_asc(value: DateTime) -> [u8; 8] {
	encode_u64_asc(value.to_bits())
}

pub fn decode_datetime_asc(bytes: [u8; 8]) -> DateTime {
	DateTime::from_bits(decode_u64_asc(bytes))
}

pub fn encode_u128_asc(value: u128) -> [u8; 16] {
	value.to_be_bytes()
}

pub fn decode_u128_asc(bytes: [u8; 16]) -> u128 {
	u128::from_be_bytes(bytes)
}

pub fn encode_u64_varint<B: ByteSink>(value: u64, output: &mut B) {
	if value < (1 << 7) {
		output.push(!(value as u8));
	} else if value < (1 << 14) {
		output.push(!(0x80 | (value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 21) {
		output.push(!(0xc0 | (value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 28) {
		output.push(!(0xe0 | (value >> 24) as u8));
		output.push(!((value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 35) {
		output.push(!(0xf0 | (value >> 32) as u8));
		output.push(!((value >> 24) as u8));
		output.push(!((value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 42) {
		output.push(!(0xf8 | (value >> 40) as u8));
		output.push(!((value >> 32) as u8));
		output.push(!((value >> 24) as u8));
		output.push(!((value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 49) {
		output.push(!(0xfc | (value >> 48) as u8));
		output.push(!((value >> 40) as u8));
		output.push(!((value >> 32) as u8));
		output.push(!((value >> 24) as u8));
		output.push(!((value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else if value < (1 << 56) {
		output.push(!(0xfe | (value >> 56) as u8));
		output.push(!((value >> 48) as u8));
		output.push(!((value >> 40) as u8));
		output.push(!((value >> 32) as u8));
		output.push(!((value >> 24) as u8));
		output.push(!((value >> 16) as u8));
		output.push(!((value >> 8) as u8));
		output.push(!(value as u8));
	} else {
		output.push(!0xff);
		let inv = !value;
		output.extend_from_slice(&inv.to_be_bytes());
	}
}

pub fn encode_u128(value: u128) -> [u8; 16] {
	(!value).to_be_bytes()
}

pub fn decode_u128(bytes: [u8; 16]) -> u128 {
	!u128::from_be_bytes(bytes)
}

pub fn encode_fixed<const N: usize>(value: [u8; N]) -> [u8; N] {
	let mut encoded = value;
	for byte in encoded.iter_mut() {
		*byte = !*byte;
	}
	encoded
}

pub fn decode_fixed<const N: usize>(bytes: [u8; N]) -> [u8; N] {
	encode_fixed(bytes)
}

pub fn encode_u128_varint<B: ByteSink>(value: u128, output: &mut B) {
	if value < (1 << 56) {
		encode_u64_varint(value as u64, output);
	} else {
		output.push(!0xff);
		let bytes = value.to_be_bytes();
		let start = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len() - 1);
		let sig = &bytes[start..];
		output.push(!(sig.len() as u8));
		for &b in sig {
			output.push(!b);
		}
	}
}

pub fn decode_u128_varint(input: &mut &[u8]) -> Result<u128> {
	if input.is_empty() {
		return Err(Error::from(TypeError::SerdeKeycode {
			message: "unexpected end of key while decoding u128 varint".to_string(),
		}));
	}
	let first = !input[0];
	let prefix = first.leading_ones() as usize;
	if prefix < 8 {
		let len = prefix + 1;
		if input.len() < len {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: "unexpected end of key while decoding u128 varint".to_string(),
			}));
		}
		let mut buf = [0u8; 9];
		for (dst, &src) in buf[..len].iter_mut().zip(&input[..len]) {
			*dst = !src;
		}
		let mut slice = &buf[..len];
		let v = varint::decode_u64_varint(&mut slice).ok_or_else(|| {
			Error::from(TypeError::SerdeKeycode {
				message: "failed to decode u128 varint".to_string(),
			})
		})?;
		*input = &input[len..];
		Ok(v as u128)
	} else {
		if input.len() < 2 {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: "unexpected end of key while decoding u128 varint length".to_string(),
			}));
		}
		let len = (!input[1]) as usize;
		if len == 0 || len > 16 || input.len() < 2 + len {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: "invalid u128 varint length".to_string(),
			}));
		}
		let mut bytes = [0u8; 16];
		for (i, &src) in input[2..2 + len].iter().enumerate() {
			bytes[16 - len + i] = !src;
		}
		*input = &input[2 + len..];
		Ok(u128::from_be_bytes(bytes))
	}
}

pub const CONTAINER_END: u8 = 0xff;

pub fn encode_bytes<B: ByteSink>(bytes: &[u8], output: &mut B) {
	let mut start = 0;
	while let Some(pos) = bytes[start..].iter().position(|&b| b == 0xff) {
		let end = start + pos;
		output.extend_from_slice(&bytes[start..end]);
		output.extend_from_slice(&[0xff, 0x00]);
		start = end + 1;
	}
	output.extend_from_slice(&bytes[start..]);
	output.extend_from_slice(&[0xff, 0xff]);
}

#[macro_export]
macro_rules! key_prefix {
    ($($arg:tt)*) => {
        &EncodedKey::new((&format!($($arg)*)).as_bytes())
    };
}
