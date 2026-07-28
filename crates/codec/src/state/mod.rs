// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::BTreeMap, mem};

use reifydb_value::{
	byte_size::ByteSize,
	error::{Error as ValueError, TypeError},
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

use crate::encoded::{
	row::EncodedRow,
	shape::{OPERATOR_STATE_SHAPE, RowShape, fingerprint::RowShapeFingerprint},
};

const STATE_FIELD: usize = 0;
const FORMAT_FIELD: usize = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateFormatVersion(pub u8);

impl StateFormatVersion {
	pub const CURRENT: Self = Self(1);
}

impl From<StateError> for ValueError {
	fn from(err: StateError) -> Self {
		match err {
			StateError::Serialization(_) => TypeError::SerdeSerialize {
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
pub enum StateError {
	#[error("operator state serialization failed: {0}")]
	Serialization(String),

	#[error("operator state validation failed: {0}")]
	Validation(String),

	#[error("operator state deserialization failed: {0}")]
	Deserialization(String),

	#[error("operator state row carries shape fingerprint {actual:?} instead of the operator state shape")]
	UnexpectedObject {
		actual: RowShapeFingerprint,
	},

	#[error("operator state payload format {0} is newer than this build supports")]
	UnsupportedFormat(u8),
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateBytes {
	row: EncodedRow,
}

impl StateBytes {
	pub fn from_row(row: EncodedRow) -> Result<Self, StateError> {
		let shape = &*OPERATOR_STATE_SHAPE;
		if row.fingerprint() != shape.fingerprint() {
			return Err(StateError::UnexpectedObject {
				actual: row.fingerprint(),
			});
		}
		let format = shape.get::<u8>(&row, FORMAT_FIELD);
		if format != StateFormatVersion::CURRENT.0 {
			return Err(StateError::UnsupportedFormat(format));
		}
		Ok(Self {
			row,
		})
	}

	pub fn from_archive(body: &[u8], now: DateTime) -> Self {
		let shape = &*OPERATOR_STATE_SHAPE;
		let mut row = shape.allocate();
		shape.set::<u8>(&mut row, FORMAT_FIELD, StateFormatVersion::CURRENT.0);
		shape.set_blob_from_slice(&mut row, STATE_FIELD, body);
		row.set_timestamps(now, now);
		Self {
			row,
		}
	}

	pub fn into_row(self) -> EncodedRow {
		self.row
	}

	pub fn row(&self) -> &EncodedRow {
		&self.row
	}

	pub fn format(&self) -> StateFormatVersion {
		StateFormatVersion(OPERATOR_STATE_SHAPE.get::<u8>(&self.row, FORMAT_FIELD))
	}

	pub fn body(&self) -> &[u8] {
		OPERATOR_STATE_SHAPE.get_blob_slice(&self.row, STATE_FIELD)
	}

	pub fn body_mut(&mut self) -> &mut [u8] {
		OPERATOR_STATE_SHAPE.get_blob_slice_mut(&mut self.row, STATE_FIELD)
	}

	pub fn refresh_updated_at(&mut self, now: DateTime) {
		let created_at = self.row.created_at();
		self.row.set_timestamps(created_at, now);
	}

	pub fn byte_size(&self) -> ByteSize {
		ByteSize::from(self.row.len() as u64)
	}
}

pub trait OperatorState: Sized + Send + 'static {
	type Archived;

	fn encode_state(&self, now: DateTime) -> Result<StateBytes, StateError>;

	fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError>;

	/// # Safety
	///
	/// `bytes` must previously have passed [`OperatorState::archived`]
	/// validation, or have been produced by [`OperatorState::encode_state`]
	/// for exactly `Self`; otherwise the archived access reads
	/// unvalidated memory through mismatched layout.
	unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived;

	/// # Safety
	///
	/// Same contract as [`OperatorState::archived_trusted`]; `bytes` must
	/// hold a validated archive of exactly `Self`. Writes through the
	/// returned [`Seal`] cannot invalidate the archive.
	unsafe fn archived_seal_trusted(bytes: &mut StateBytes) -> Seal<'_, Self::Archived>;

	fn materialize(archived: &Self::Archived) -> Result<Self, StateError>;
}

pub trait SealMutableState: OperatorState {}

thread_local! {
	static ENCODE_BUFFER: RefCell<AlignedVec> = RefCell::new(AlignedVec::new());
}

pub fn encode_archive<T>(value: &T, now: DateTime) -> Result<StateBytes, StateError>
where
	T: for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
{
	let buffer = ENCODE_BUFFER.with(|cell| mem::take(&mut *cell.borrow_mut()));
	let mut filled =
		to_bytes_in::<_, RancorError>(value, buffer).map_err(|e| StateError::Serialization(e.to_string()))?;
	let result = StateBytes::from_archive(filled.as_slice(), now);
	filled.clear();
	ENCODE_BUFFER.with(|cell| *cell.borrow_mut() = filled);
	Ok(result)
}

pub fn access_archive<T>(bytes: &StateBytes) -> Result<&T::Archived, StateError>
where
	T: Archive,
	T::Archived: Portable + for<'a> CheckBytes<HighValidator<'a, RancorError>>,
{
	access::<T::Archived, RancorError>(bytes.body()).map_err(|e| StateError::Validation(e.to_string()))
}

/// # Safety
///
/// `bytes` must hold an archive of exactly `T` that has either been produced
/// by [`encode_archive`] in this process or passed [`access_archive`]
/// validation; the unchecked access relies on that for pointer and layout
/// validity.
pub unsafe fn access_archive_trusted<T>(bytes: &StateBytes) -> &T::Archived
where
	T: Archive,
	T::Archived: Portable,
{
	// SAFETY: forwarded contract; see the function-level Safety section.
	unsafe { access_unchecked::<T::Archived>(bytes.body()) }
}

/// # Safety
///
/// Same contract as [`access_archive_trusted`]; `bytes` must hold a validated
/// archive of exactly `T`. Seal writes cannot invalidate the archive.
pub unsafe fn access_archive_seal_trusted<T>(bytes: &mut StateBytes) -> Seal<'_, T::Archived>
where
	T: Archive,
	T::Archived: Portable,
{
	// SAFETY: forwarded contract; see the function-level Safety section.
	unsafe { access_unchecked_mut::<T::Archived>(bytes.body_mut()) }
}

pub fn materialize_archive<T>(archived: &T::Archived) -> Result<T, StateError>
where
	T: Archive,
	T::Archived: RkyvDeserialize<T, Strategy<Pool, RancorError>>,
{
	deserialize::<T, RancorError>(archived).map_err(|e| StateError::Deserialization(e.to_string()))
}

pub fn decode_state<T: OperatorState>(bytes: &StateBytes) -> Result<T, StateError> {
	T::materialize(T::archived(bytes)?)
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

			fn encode_state(&self, now: DateTime) -> Result<StateBytes, StateError> {
				encode_archive(self, now)
			}

			fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError> {
				access_archive::<Self>(bytes)
			}

			unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived {
				// SAFETY: forwarded contract; see OperatorState::archived_trusted.
				unsafe { access_archive_trusted::<Self>(bytes) }
			}

			unsafe fn archived_seal_trusted(bytes: &mut StateBytes) -> Seal<'_, Self::Archived> {
				// SAFETY: forwarded contract; see OperatorState::archived_seal_trusted.
				unsafe { access_archive_seal_trusted::<Self>(bytes) }
			}

			fn materialize(archived: &Self::Archived) -> Result<Self, StateError> {
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

	fn encode_state(&self, now: DateTime) -> Result<StateBytes, StateError> {
		encode_archive(self, now)
	}

	fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError> {
		access_archive::<Self>(bytes)
	}

	unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived {
		// SAFETY: forwarded contract; see OperatorState::archived_trusted.
		unsafe { access_archive_trusted::<Self>(bytes) }
	}

	unsafe fn archived_seal_trusted(bytes: &mut StateBytes) -> Seal<'_, Self::Archived> {
		// SAFETY: forwarded contract; see OperatorState::archived_seal_trusted.
		unsafe { access_archive_seal_trusted::<Self>(bytes) }
	}

	fn materialize(archived: &Self::Archived) -> Result<Self, StateError> {
		materialize_archive::<Self>(archived)
	}
}

pub fn operator_state_shape() -> &'static RowShape {
	&OPERATOR_STATE_SHAPE
}

#[cfg(test)]
mod tests {
	use std::mem::align_of;

	use reifydb_value::value::{datetime::DateTime, value_type::ValueType};
	use rkyv::{
		Archive, Deserialize, Serialize, access,
		primitive::{ArchivedF64, ArchivedI64, ArchivedU64},
		rancor::Error as TestRancorError,
	};

	use super::{
		StateBytes, StateError, StateFormatVersion, access_archive, access_archive_trusted, encode_archive,
		materialize_archive, operator_state_shape,
	};
	use crate::encoded::shape::RowShape;

	#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
	struct Probe {
		total: u64,
		names: Vec<String>,
	}

	fn at(nanos: u64) -> DateTime {
		DateTime::from_nanos(nanos)
	}

	fn probe() -> Probe {
		Probe {
			total: 42,
			names: vec!["a".to_string(), "bb".to_string()],
		}
	}

	#[test]
	fn test_encode_access_materialize_round_trip() {
		// The full path a state value takes: encode at flush, validate
		// once at the trust boundary, read archived without decode,
		// materialize on promotion. Every step must be lossless.
		let value = probe();
		let bytes = encode_archive(&value, at(7)).unwrap();

		assert_eq!(bytes.format(), StateFormatVersion::CURRENT);

		let archived = access_archive::<Probe>(&bytes).unwrap();
		assert_eq!(archived.total, 42);
		assert_eq!(archived.names.len(), 2);
		assert_eq!(archived.names[1].as_str(), "bb");

		let restored: Probe = materialize_archive(archived).unwrap();
		assert_eq!(restored, value);

		// SAFETY: bytes passed access_archive validation above.
		let trusted = unsafe { access_archive_trusted::<Probe>(&bytes) };
		assert_eq!(trusted.total, 42);
	}

	#[test]
	fn test_refresh_updated_at_preserves_created_at_and_body() {
		// The seal-flush path stamps a fresh updated_at on bytes it writes
		// verbatim. set_timestamps writes BOTH header timestamps, so
		// refresh_updated_at must re-read created_at first; clobbering it
		// would corrupt TTL semantics for sealed entries.
		let value = probe();
		let mut bytes = encode_archive(&value, at(7)).unwrap();
		assert_eq!(bytes.row().created_at(), at(7));
		assert_eq!(bytes.row().updated_at(), at(7));

		bytes.refresh_updated_at(at(99));
		assert_eq!(bytes.row().created_at(), at(7), "refresh must not clobber created_at");
		assert_eq!(bytes.row().updated_at(), at(99));
		assert_eq!(access_archive::<Probe>(&bytes).unwrap().total, 42, "the body must stay untouched");
	}

	#[test]
	fn test_body_mut_windows_the_same_bytes_as_body() {
		// body_mut is the seal path's write window; it must expose exactly
		// the blob body (offset and length) that body() reads, or sealed
		// writes would land outside the archive.
		let value = probe();
		let mut bytes = encode_archive(&value, DateTime::EPOCH).unwrap();
		let body = bytes.body().to_vec();
		assert_eq!(bytes.body_mut(), &body[..]);
		assert_eq!(bytes.body(), &body[..]);
	}

	#[test]
	fn test_archived_access_is_alignment_free() {
		// The archive body sits at an arbitrary byte offset inside plain
		// Vec<u8> row buffers on every tier (read buffer, persistent store,
		// FFI copies), so the soundness of archived access rests entirely on
		// rkyv's "unaligned" feature. The const asserts fail to compile if a
		// future rkyv bump drops the feature (archived primitives would regain
		// alignment > 1); the loop pins that validated access and
		// materialization round-trip from every misaligned offset.
		const _: () = assert!(align_of::<ArchivedU64>() == 1);
		const _: () = assert!(align_of::<ArchivedI64>() == 1);
		const _: () = assert!(align_of::<ArchivedF64>() == 1);

		let value = probe();
		let bytes = encode_archive(&value, at(7)).unwrap();
		let body = bytes.body().to_vec();
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
	fn test_row_round_trip_preserves_timestamps() {
		// StateBytes must survive the into_row/from_row boundary it
		// crosses on every store write and read, keeping the row
		// header timestamps that TTL semantics depend on.
		let bytes = encode_archive(&probe(), at(1234)).unwrap();
		let row = bytes.clone().into_row();
		assert_eq!(row.created_at(), at(1234));

		let reloaded = StateBytes::from_row(row).unwrap();
		assert_eq!(reloaded, bytes);
		let archived = access_archive::<Probe>(&reloaded).unwrap();
		assert_eq!(archived.total, 42);
	}

	#[test]
	fn test_from_row_rejects_foreign_shape() {
		// A row written under any other shape must be rejected with
		// the fingerprint diagnostic, not misread as state bytes.
		let foreign = RowShape::testing(&[ValueType::Int8]).allocate();
		let err = StateBytes::from_row(foreign).unwrap_err();
		assert!(matches!(err, StateError::UnexpectedObject { .. }));
	}

	#[test]
	fn test_from_row_rejects_unknown_format() {
		// A zeroed format byte (how a legacy postcard writer would
		// leave it) and a future format byte must both fail loudly.
		let shape = operator_state_shape();
		let row = shape.allocate();
		let err = StateBytes::from_row(row).unwrap_err();
		assert_eq!(err, StateError::UnsupportedFormat(0));

		let mut future = shape.allocate();
		shape.set::<u8>(&mut future, 1, 9u8);
		let err = StateBytes::from_row(future).unwrap_err();
		assert_eq!(err, StateError::UnsupportedFormat(9));
	}

	#[test]
	fn test_truncated_body_fails_validation() {
		// bytecheck must reject a corrupted body as an error rather
		// than panic; this is the disk-corruption trust boundary.
		let bytes = encode_archive(&probe(), DateTime::EPOCH).unwrap();
		let body = bytes.body();
		let truncated = StateBytes::from_archive(&body[..body.len() / 2], DateTime::EPOCH);
		assert!(matches!(access_archive::<Probe>(&truncated), Err(StateError::Validation(_))));
	}

	#[test]
	fn test_byte_size_covers_whole_row() {
		let bytes = encode_archive(&probe(), DateTime::EPOCH).unwrap();
		assert_eq!(bytes.byte_size().as_bytes(), bytes.row().len() as u64);
		assert!(bytes.byte_size().as_bytes() > bytes.body().len() as u64);
	}

	#[test]
	fn test_from_archive_body_round_trips_exactly() {
		let payload: Vec<u8> = (0..=255).collect();
		let bytes = StateBytes::from_archive(&payload, at(7));
		assert_eq!(bytes.body(), payload.as_slice());
		assert_eq!(bytes.format(), StateFormatVersion::CURRENT);
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
		let big_bytes = encode_archive(&big, DateTime::EPOCH).unwrap();
		let small_bytes = encode_archive(&small, DateTime::EPOCH).unwrap();
		assert!(small_bytes.body().len() < big_bytes.body().len());
		let restored: Probe =
			materialize_archive::<Probe>(access_archive::<Probe>(&small_bytes).unwrap()).unwrap();
		assert_eq!(restored, small);
		let restored_big: Probe =
			materialize_archive::<Probe>(access_archive::<Probe>(&big_bytes).unwrap()).unwrap();
		assert_eq!(restored_big, big);
	}
}
