// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::BTreeMap, mem, ops::Deref};

use reifydb_value::{
	byte_size::ByteSize,
	encoding::LeBytes,
	error::{Error as ValueError, TypeError},
	util::cowvec::CowVec,
	value::datetime::DateTime,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::row::bytes::{EncodedBytes, EncodedRowBuilder, RowBuilder, read_defined_at, sealed::Sealed};

const TIME_OFFSET: usize = 0;

pub const OPERATOR_HEADER_SIZE: usize = TIME_OFFSET + DateTime::ENCODED_SIZE;

impl From<OperatorError> for ValueError {
	fn from(err: OperatorError) -> Self {
		match err {
			OperatorError::Serialization(_) => TypeError::SerdeSerialize {
				message: err.to_string(),
			}
			.into(),
			_ => TypeError::SerdeDeserialize {
				message: err.to_string(),
			}
			.into(),
		}
	}
}

#[derive(Debug, Error, PartialEq)]
pub enum OperatorError {
	#[error("operator state serialization failed: {0}")]
	Serialization(String),

	#[error("operator state deserialization failed: {0}")]
	Deserialization(String),

	#[error("operator row is {len} bytes, too short to carry the time header")]
	Truncated {
		len: usize,
	},
}

#[inline]
pub fn read_time(buf: &[u8]) -> Option<DateTime> {
	let time = DateTime::from_le_bytes(
		buf[TIME_OFFSET..OPERATOR_HEADER_SIZE].try_into().expect("the operator header is length-checked"),
	);
	(time != DateTime::MAX).then_some(time)
}

#[inline]
pub fn write_time(buf: &mut [u8], time: DateTime) {
	buf[TIME_OFFSET..OPERATOR_HEADER_SIZE].copy_from_slice(&time.to_le_bytes());
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedOperatorRow(EncodedBytes);

impl EncodedOperatorRow {
	pub fn new(body: &[u8], time: DateTime) -> Self {
		let mut buffer = Vec::with_capacity(OPERATOR_HEADER_SIZE + body.len());
		buffer.extend_from_slice(&time.to_le_bytes());
		buffer.extend_from_slice(body);
		Self(EncodedBytes(CowVec::new(buffer)))
	}

	pub fn timeless(body: &[u8]) -> Self {
		Self::new(body, DateTime::MAX)
	}

	pub fn into_bytes(self) -> EncodedBytes {
		self.0
	}

	pub fn bytes(&self) -> &EncodedBytes {
		&self.0
	}

	pub fn view(bytes: &EncodedBytes) -> &Self {
		// SAFETY: EncodedOperatorRow is repr(transparent) over EncodedBytes, so the pointer cast
		// preserves layout, and the returned reference borrows the same allocation for the same lifetime.
		unsafe { &*(bytes as *const EncodedBytes as *const Self) }
	}

	#[inline]
	pub fn row_time(&self) -> Option<DateTime> {
		read_time(&self.0)
	}

	#[inline]
	pub fn time(&self) -> DateTime {
		DateTime::from_le_bytes(
			self.0[TIME_OFFSET..OPERATOR_HEADER_SIZE].try_into().expect("the header is length-checked"),
		)
	}

	pub fn set_time(&mut self, time: DateTime) {
		self.0.make_mut()[TIME_OFFSET..OPERATOR_HEADER_SIZE].copy_from_slice(&time.to_le_bytes());
	}

	pub fn body(&self) -> &[u8] {
		&self.0[OPERATOR_HEADER_SIZE..]
	}

	pub fn body_mut(&mut self) -> &mut [u8] {
		&mut self.0.make_mut()[OPERATOR_HEADER_SIZE..]
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.body().is_empty()
	}

	pub fn byte_size(&self) -> ByteSize {
		ByteSize::from(self.0.len() as u64)
	}
}

impl TryFrom<EncodedBytes> for EncodedOperatorRow {
	type Error = OperatorError;

	fn try_from(bytes: EncodedBytes) -> Result<Self, Self::Error> {
		if bytes.len() < OPERATOR_HEADER_SIZE {
			return Err(OperatorError::Truncated {
				len: bytes.len(),
			});
		}
		Ok(Self(bytes))
	}
}

impl From<EncodedOperatorRow> for EncodedBytes {
	fn from(row: EncodedOperatorRow) -> Self {
		row.0
	}
}

/// The write side of the operator family: a buffer already carrying a time header, which freezes
/// into an [`EncodedOperatorRow`] and never into a row of another family.
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedOperatorRowBuilder(EncodedRowBuilder);

impl EncodedOperatorRowBuilder {
	pub(crate) fn wrap(builder: EncodedRowBuilder) -> Self {
		Self(builder)
	}

	#[inline]
	pub fn row_time(&self) -> Option<DateTime> {
		read_time(self.as_slice())
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_time(self.as_mut_slice(), time);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.as_slice(), OPERATOR_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.as_slice()[OPERATOR_HEADER_SIZE..]
	}

	pub fn freeze(self) -> EncodedOperatorRow {
		EncodedOperatorRow(self.0.freeze())
	}
}

impl Sealed for EncodedOperatorRowBuilder {
	fn buffer(&self) -> &Vec<u8> {
		self.0.buffer()
	}

	fn buffer_mut(&mut self) -> &mut Vec<u8> {
		self.0.buffer_mut()
	}

	fn take_buffer(self) -> Vec<u8> {
		self.0.take_buffer()
	}
}

impl EncodedOperatorRow {
	pub fn thaw(self) -> EncodedOperatorRowBuilder {
		EncodedOperatorRowBuilder(self.0.thaw())
	}
}

pub trait OperatorState: Sized + Send + 'static {
	fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError>;

	fn decode_state(row: &EncodedOperatorRow) -> Result<Self, OperatorError>;
}

thread_local! {
	static ENCODE_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn encode<T>(value: &T, now: DateTime) -> Result<EncodedOperatorRow, OperatorError>
where
	T: Serialize + ?Sized,
{
	let mut buffer = ENCODE_BUFFER.with(|cell| mem::take(&mut *cell.borrow_mut()));
	buffer.clear();
	let mut filled = postcard::to_extend(value, buffer).map_err(|e| OperatorError::Serialization(e.to_string()))?;
	let result = EncodedOperatorRow::new(&filled, now);
	filled.clear();
	ENCODE_BUFFER.with(|cell| *cell.borrow_mut() = filled);
	Ok(result)
}

pub fn decode_body<T>(row: &EncodedOperatorRow) -> Result<T, OperatorError>
where
	T: DeserializeOwned,
{
	postcard::from_bytes(row.body()).map_err(|e| OperatorError::Deserialization(e.to_string()))
}

pub fn decode<T: OperatorState>(row: &EncodedOperatorRow) -> Result<T, OperatorError> {
	T::decode_state(row)
}

pub mod derive {
	pub use serde::{self, Deserialize, Serialize};
}

pub trait StateCodec: Sized + Send + 'static + Serialize + DeserializeOwned {}

impl<T> StateCodec for T where T: Sized + Send + 'static + Serialize + DeserializeOwned {}

macro_rules! leaf_operator_state {
	($($ty:ty),* $(,)?) => {
		$(impl OperatorState for $ty {
			fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError> {
				encode(self, now)
			}

			fn decode_state(row: &EncodedOperatorRow) -> Result<Self, OperatorError> {
				decode_body::<Self>(row)
			}
		})*
	};
}

leaf_operator_state!(u64, i64, Vec<u8>, (i64, i64, i64));

impl<K, V> OperatorState for BTreeMap<K, V>
where
	K: Send + 'static,
	V: Send + 'static,
	Self: Serialize + DeserializeOwned,
{
	fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError> {
		encode(self, now)
	}

	fn decode_state(row: &EncodedOperatorRow) -> Result<Self, OperatorError> {
		decode_body::<Self>(row)
	}
}

impl Deref for EncodedOperatorRowBuilder {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.as_slice()
	}
}
