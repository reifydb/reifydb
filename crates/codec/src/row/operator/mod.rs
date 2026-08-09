// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::BTreeMap, mem};

use reifydb_value::{
	byte_size::ByteSize,
	encoding::LeBytes,
	error::{Error as ValueError, TypeError},
	util::cowvec::CowVec,
	value::datetime::DateTime,
};
use rkyv::{
	Archive, Deserialize as RkyvDeserialize, Portable, Serialize as RkyvSerialize, access, access_unchecked,
	access_unchecked_mut,
	api::high::{HighSerializer, HighValidator, to_bytes_in},
	bytecheck::CheckBytes,
	de::Pool,
	deserialize,
	rancor::{Error as RancorError, Strategy},
	seal::Seal,
	ser::allocator::ArenaHandle,
	util::AlignedVec,
};
use thiserror::Error;

use crate::row::bytes::{EncodedBytes, EncodedRowBuilder, read_defined_at};

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

	#[error("operator state validation failed: {0}")]
	Validation(String),

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

	pub fn builder(&self) -> &EncodedRowBuilder {
		&self.0
	}

	pub fn builder_mut(&mut self) -> &mut EncodedRowBuilder {
		&mut self.0
	}

	pub fn as_slice(&self) -> &[u8] {
		self.0.as_slice()
	}

	pub fn as_mut_slice(&mut self) -> &mut [u8] {
		self.0.as_mut_slice()
	}

	#[inline]
	pub fn row_time(&self) -> Option<DateTime> {
		read_time(self.0.as_slice())
	}

	pub fn set_time(&mut self, time: DateTime) {
		write_time(self.0.as_mut_slice(), time);
	}

	#[inline]
	pub fn is_defined(&self, index: usize) -> bool {
		read_defined_at(self.0.as_slice(), OPERATOR_HEADER_SIZE, index)
	}

	pub fn body(&self) -> &[u8] {
		&self.0.as_slice()[OPERATOR_HEADER_SIZE..]
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.body().is_empty()
	}

	pub fn freeze(self) -> EncodedOperatorRow {
		EncodedOperatorRow(self.0.freeze())
	}
}

pub trait OperatorState: Sized + Send + 'static {
	type Archived;

	fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError>;

	fn archived(row: &EncodedOperatorRow) -> Result<&Self::Archived, OperatorError>;

	/// # Safety
	///
	/// `row` must previously have passed [`OperatorState::archived`]
	/// validation, or have been produced by [`OperatorState::encode_state`]
	/// for exactly `Self`; otherwise the archived access reads
	/// unvalidated memory through mismatched layout.
	unsafe fn archived_trusted(row: &EncodedOperatorRow) -> &Self::Archived;

	/// # Safety
	///
	/// Same contract as [`OperatorState::archived_trusted`]; `row` must
	/// hold a validated archive of exactly `Self`. Writes through the
	/// returned [`Seal`] cannot invalidate the archive.
	unsafe fn archived_seal_trusted(row: &mut EncodedOperatorRow) -> Seal<'_, Self::Archived>;

	fn materialize(archived: &Self::Archived) -> Result<Self, OperatorError>;
}

pub trait SealMutableState: OperatorState {}

thread_local! {
	static ENCODE_BUFFER: RefCell<AlignedVec> = RefCell::new(AlignedVec::new());
}

pub fn encode_archive<T>(value: &T, now: DateTime) -> Result<EncodedOperatorRow, OperatorError>
where
	T: for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
{
	let buffer = ENCODE_BUFFER.with(|cell| mem::take(&mut *cell.borrow_mut()));
	let mut filled = to_bytes_in::<_, RancorError>(value, buffer)
		.map_err(|e| OperatorError::Serialization(e.to_string()))?;
	let result = EncodedOperatorRow::new(filled.as_slice(), now);
	filled.clear();
	ENCODE_BUFFER.with(|cell| *cell.borrow_mut() = filled);
	Ok(result)
}

pub fn access_archive<T>(row: &EncodedOperatorRow) -> Result<&T::Archived, OperatorError>
where
	T: Archive,
	T::Archived: Portable + for<'a> CheckBytes<HighValidator<'a, RancorError>>,
{
	access::<T::Archived, RancorError>(row.body()).map_err(|e| OperatorError::Validation(e.to_string()))
}

/// # Safety
///
/// `row` must hold an archive of exactly `T` that has either been produced
/// by [`encode_archive`] in this process or passed [`access_archive`]
/// validation; the unchecked access relies on that for pointer and layout
/// validity.
pub unsafe fn access_archive_trusted<T>(row: &EncodedOperatorRow) -> &T::Archived
where
	T: Archive,
	T::Archived: Portable,
{
	// SAFETY: the caller guarantees `row` holds a validated archive of exactly `T`, so the
	// body is a well-formed `T::Archived` with valid relative pointers.
	unsafe { access_unchecked::<T::Archived>(row.body()) }
}

/// # Safety
///
/// Same contract as [`access_archive_trusted`]; `row` must hold a validated
/// archive of exactly `T`. Seal writes cannot invalidate the archive.
pub unsafe fn access_archive_seal_trusted<T>(row: &mut EncodedOperatorRow) -> Seal<'_, T::Archived>
where
	T: Archive,
	T::Archived: Portable,
{
	// SAFETY: the caller guarantees `row` holds a validated archive of exactly `T`, and the
	// exclusive borrow makes the sealed mutable access non-aliasing.
	unsafe { access_unchecked_mut::<T::Archived>(row.body_mut()) }
}

pub fn materialize_archive<T>(archived: &T::Archived) -> Result<T, OperatorError>
where
	T: Archive,
	T::Archived: RkyvDeserialize<T, Strategy<Pool, RancorError>>,
{
	deserialize::<T, RancorError>(archived).map_err(|e| OperatorError::Deserialization(e.to_string()))
}

pub fn decode<T: OperatorState>(row: &EncodedOperatorRow) -> Result<T, OperatorError> {
	T::materialize(T::archived(row)?)
}

pub mod archive {
	pub use rkyv::{self, Archive, Deserialize, Serialize};
}

pub trait ArchiveState:
	Sized
	+ Send
	+ 'static
	+ for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
	+ Archive<
		Archived: Portable
		                  + for<'a> CheckBytes<HighValidator<'a, RancorError>>
		                  + RkyvDeserialize<Self, Strategy<Pool, RancorError>>,
	>
{
}

impl<T> ArchiveState for T where
	T: Send
		+ 'static
		+ for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
		+ Archive<
			Archived: Portable
			                  + for<'a> CheckBytes<HighValidator<'a, RancorError>>
			                  + RkyvDeserialize<T, Strategy<Pool, RancorError>>,
		>
{
}

macro_rules! leaf_operator_state {
	($($ty:ty),* $(,)?) => {
		$(impl OperatorState for $ty {
			type Archived = <$ty as Archive>::Archived;

			fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError> {
				encode_archive(self, now)
			}

			fn archived(row: &EncodedOperatorRow) -> Result<&Self::Archived, OperatorError> {
				access_archive::<Self>(row)
			}

			unsafe fn archived_trusted(row: &EncodedOperatorRow) -> &Self::Archived {
				// SAFETY: the caller of archived_trusted guarantees `row` holds a
				// validated archive of exactly `Self`, which is what the callee needs.
				unsafe { access_archive_trusted::<Self>(row) }
			}

			unsafe fn archived_seal_trusted(row: &mut EncodedOperatorRow) -> Seal<'_, Self::Archived> {
				// SAFETY: the caller of archived_seal_trusted guarantees `row` holds a
				// validated archive of exactly `Self`, which is what the callee needs.
				unsafe { access_archive_seal_trusted::<Self>(row) }
			}

			fn materialize(archived: &Self::Archived) -> Result<Self, OperatorError> {
				materialize_archive::<Self>(archived)
			}
		})*
	};
}

leaf_operator_state!(u64, i64, Vec<u8>, (i64, i64, i64));

impl<K, V> OperatorState for BTreeMap<K, V>
where
	K: Send + 'static,
	V: Send + 'static,
	Self: for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
	Self: Archive,
	<Self as Archive>::Archived: Portable
		+ for<'a> CheckBytes<HighValidator<'a, RancorError>>
		+ RkyvDeserialize<Self, Strategy<Pool, RancorError>>,
{
	type Archived = <Self as Archive>::Archived;

	fn encode_state(&self, now: DateTime) -> Result<EncodedOperatorRow, OperatorError> {
		encode_archive(self, now)
	}

	fn archived(row: &EncodedOperatorRow) -> Result<&Self::Archived, OperatorError> {
		access_archive::<Self>(row)
	}

	unsafe fn archived_trusted(row: &EncodedOperatorRow) -> &Self::Archived {
		// SAFETY: the caller of archived_trusted guarantees `row` holds a validated archive
		// of exactly `Self`, which is what the callee needs.
		unsafe { access_archive_trusted::<Self>(row) }
	}

	unsafe fn archived_seal_trusted(row: &mut EncodedOperatorRow) -> Seal<'_, Self::Archived> {
		// SAFETY: the caller of archived_seal_trusted guarantees `row` holds a validated
		// archive of exactly `Self`, which is what the callee needs.
		unsafe { access_archive_seal_trusted::<Self>(row) }
	}

	fn materialize(archived: &Self::Archived) -> Result<Self, OperatorError> {
		materialize_archive::<Self>(archived)
	}
}
