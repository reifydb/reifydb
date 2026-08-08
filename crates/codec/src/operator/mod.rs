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

use crate::encoded::bytes::EncodedBytes;

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

#[cfg(test)]
mod tests {
	use std::mem::align_of;

	use reifydb_value::{factory::time::at_nanos, util::cowvec::CowVec, value::datetime::DateTime};
	use rkyv::{
		Archive, Deserialize, Serialize, access,
		primitive::{ArchivedF64, ArchivedI64, ArchivedU64},
		rancor::Error as TestRancorError,
	};

	use super::{
		EncodedOperatorRow, OPERATOR_HEADER_SIZE, OperatorError, access_archive, access_archive_trusted,
		encode_archive, materialize_archive,
	};
	use crate::encoded::bytes::EncodedBytes;

	#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
	struct Probe {
		total: u64,
		names: Vec<String>,
	}

	fn probe() -> Probe {
		Probe {
			total: 42,
			names: vec!["a".to_string(), "bb".to_string()],
		}
	}

	#[test]
	fn test_encode_access_materialize_round_trip() {
		// Encode -> validate -> read archived -> materialize must be lossless at every step.
		let value = probe();
		let row = encode_archive(&value, at_nanos(7)).unwrap();

		let archived = access_archive::<Probe>(&row).unwrap();
		assert_eq!(archived.total, 42);
		assert_eq!(archived.names.len(), 2);
		assert_eq!(archived.names[1].as_str(), "bb");

		let restored: Probe = materialize_archive(archived).unwrap();
		assert_eq!(restored, value);

		// SAFETY: row passed access_archive validation above.
		let trusted = unsafe { access_archive_trusted::<Probe>(&row) };
		assert_eq!(trusted.total, 42);
	}

	#[test]
	fn test_set_time_preserves_body() {
		// set_time must write exactly the header window, never the first byte of the archive.
		let value = probe();
		let mut row = encode_archive(&value, at_nanos(7)).unwrap();
		assert_eq!(row.time(), at_nanos(7));
		let body = row.body().to_vec();

		row.set_time(at_nanos(99));
		assert_eq!(row.time(), at_nanos(99));
		assert_eq!(row.body(), &body[..], "the body must stay untouched");
		assert_eq!(access_archive::<Probe>(&row).unwrap().total, 42);
	}

	#[test]
	fn test_body_mut_windows_the_same_bytes_as_body() {
		// body_mut must window exactly body(), otherwise sealed writes land outside the archive.
		let value = probe();
		let mut row = encode_archive(&value, DateTime::EPOCH).unwrap();
		let body = row.body().to_vec();
		assert_eq!(row.body_mut(), &body[..]);
		assert_eq!(row.body(), &body[..]);
	}

	#[test]
	fn test_archived_access_is_alignment_free() {
		// The body starts at byte 8 of a plain Vec, so archived primitives must stay align-1.
		const _: () = assert!(align_of::<ArchivedU64>() == 1);
		const _: () = assert!(align_of::<ArchivedI64>() == 1);
		const _: () = assert!(align_of::<ArchivedF64>() == 1);

		let value = probe();
		let row = encode_archive(&value, at_nanos(7)).unwrap();
		let body = row.body().to_vec();
		for offset in 1..8usize {
			let mut buffer = vec![0u8; offset];
			buffer.extend_from_slice(&body);
			let archived =
				access::<ArchivedProbe, TestRancorError>(&buffer[offset..]).unwrap_or_else(|e| {
					panic!("archived access must not require alignment (offset {offset}): {e}")
				});
			assert_eq!(archived.total, 42);
			let restored: Probe = materialize_archive(archived).unwrap();
			assert_eq!(restored, value, "round trip from misaligned offset {offset}");
		}
	}

	#[test]
	fn test_row_round_trip_preserves_time() {
		// The store boundary must preserve the time header, otherwise floor expiry reads garbage.
		let row = encode_archive(&probe(), at_nanos(1234)).unwrap();
		let encoded = row.clone().into_bytes();

		let reloaded = EncodedOperatorRow::try_from(encoded).unwrap();
		assert_eq!(reloaded, row);
		assert_eq!(reloaded.time(), at_nanos(1234));
		assert_eq!(access_archive::<Probe>(&reloaded).unwrap().total, 42);
	}

	#[test]
	fn test_try_from_rejects_a_row_too_short_to_hold_the_header() {
		// A short row must error here, otherwise time() indexes out of bounds downstream.
		for len in 0..OPERATOR_HEADER_SIZE {
			let short = EncodedBytes(CowVec::new(vec![0u8; len]));
			assert_eq!(
				EncodedOperatorRow::try_from(short).unwrap_err(),
				OperatorError::Truncated {
					len,
				}
			);
		}
	}

	#[test]
	fn test_zeroed_row_fails_archive_validation() {
		// A zeroed body must fail bytecheck rather than be read as a valid archive.
		let zeroed = EncodedOperatorRow::new(&[0u8; 16], DateTime::EPOCH);
		assert!(matches!(access_archive::<Probe>(&zeroed), Err(OperatorError::Validation(_))));
	}

	#[test]
	fn test_truncated_body_fails_validation() {
		// This is the disk-corruption trust boundary: bytecheck must error, not panic.
		let row = encode_archive(&probe(), DateTime::EPOCH).unwrap();
		let body = row.body();
		let truncated = EncodedOperatorRow::new(&body[..body.len() / 2], DateTime::EPOCH);
		assert!(matches!(access_archive::<Probe>(&truncated), Err(OperatorError::Validation(_))));
	}

	#[test]
	fn test_timeless_rows_sort_above_every_cutoff() {
		// Absence is DateTime::MAX, which must outrank any cutoff a floor sweep can propose.
		let row = EncodedOperatorRow::timeless(&[]);
		assert_eq!(row.time(), DateTime::MAX);
		assert!(row.time() > at_nanos(u64::MAX - 1));
		assert!(row.body().is_empty());
	}

	#[test]
	fn test_byte_size_covers_header_and_body() {
		let row = encode_archive(&probe(), DateTime::EPOCH).unwrap();
		assert_eq!(row.byte_size().as_bytes(), row.len() as u64);
		assert_eq!(row.len(), OPERATOR_HEADER_SIZE + row.body().len());
	}

	#[test]
	fn test_new_body_round_trips_exactly() {
		let payload: Vec<u8> = (0..=255).collect();
		let row = EncodedOperatorRow::new(&payload, at_nanos(7));
		assert_eq!(row.body(), payload.as_slice());
		assert_eq!(row.time(), at_nanos(7));
	}

	#[test]
	fn test_consecutive_encodes_share_a_buffer_without_bleed() {
		let big = Probe {
			total: 1,
			names: (0..64).map(|i| format!("name-{i}")).collect(),
		};
		let small = Probe {
			total: 2,
			names: vec!["x".to_string()],
		};
		let big_row = encode_archive(&big, DateTime::EPOCH).unwrap();
		let small_row = encode_archive(&small, DateTime::EPOCH).unwrap();
		assert!(small_row.body().len() < big_row.body().len());
		let restored: Probe =
			materialize_archive::<Probe>(access_archive::<Probe>(&small_row).unwrap()).unwrap();
		assert_eq!(restored, small);
		let restored_big: Probe =
			materialize_archive::<Probe>(access_archive::<Probe>(&big_row).unwrap()).unwrap();
		assert_eq!(restored_big, big);
	}
}
