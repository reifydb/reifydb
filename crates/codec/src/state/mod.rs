// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::{
	byte_size::ByteSize,
	error::{Error as ValueError, TypeError},
	value::blob::Blob,
};
use rkyv::{
	Archive, Deserialize as RkyvDeserialize, Portable, Serialize as RkyvSerialize, access, access_unchecked,
	api::high::{HighSerializer, HighValidator},
	bytecheck::CheckBytes,
	de::Pool,
	deserialize,
	rancor::{Error as RancorError, Strategy},
	ser::allocator::ArenaHandle,
	to_bytes,
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
	UnexpectedShape {
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
			return Err(StateError::UnexpectedShape {
				actual: row.fingerprint(),
			});
		}
		let format = shape.get_u8(&row, FORMAT_FIELD);
		if format != StateFormatVersion::CURRENT.0 {
			return Err(StateError::UnsupportedFormat(format));
		}
		Ok(Self {
			row,
		})
	}

	pub fn from_archive(body: &[u8], now_nanos: u64) -> Self {
		let shape = &*OPERATOR_STATE_SHAPE;
		let mut row = shape.allocate();
		shape.set_u8(&mut row, FORMAT_FIELD, StateFormatVersion::CURRENT.0);
		shape.set_blob(&mut row, STATE_FIELD, &Blob::from_slice(body));
		row.set_timestamps(now_nanos, now_nanos);
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
		StateFormatVersion(OPERATOR_STATE_SHAPE.get_u8(&self.row, FORMAT_FIELD))
	}

	pub fn body(&self) -> &[u8] {
		OPERATOR_STATE_SHAPE.get_blob_slice(&self.row, STATE_FIELD)
	}

	pub fn byte_size(&self) -> ByteSize {
		ByteSize::from(self.row.len() as u64)
	}
}

pub trait OperatorState: Sized + Send + 'static {
	type Archived;

	fn encode_state(&self, now_nanos: u64) -> Result<StateBytes, StateError>;

	fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError>;

	/// # Safety
	///
	/// `bytes` must previously have passed [`OperatorState::archived`]
	/// validation, or have been produced by [`OperatorState::encode_state`]
	/// for exactly `Self`; otherwise the archived access reads
	/// unvalidated memory through mismatched layout.
	unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived;

	fn materialize(archived: &Self::Archived) -> Result<Self, StateError>;
}

pub fn encode_archive<T>(value: &T, now_nanos: u64) -> Result<StateBytes, StateError>
where
	T: for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
{
	let bytes = to_bytes::<RancorError>(value).map_err(|e| StateError::Serialization(e.to_string()))?;
	Ok(StateBytes::from_archive(bytes.as_slice(), now_nanos))
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

			fn encode_state(&self, now_nanos: u64) -> Result<StateBytes, StateError> {
				encode_archive(self, now_nanos)
			}

			fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError> {
				access_archive::<Self>(bytes)
			}

			unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived {
				// SAFETY: forwarded contract; see OperatorState::archived_trusted.
				unsafe { access_archive_trusted::<Self>(bytes) }
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

	fn encode_state(&self, now_nanos: u64) -> Result<StateBytes, StateError> {
		encode_archive(self, now_nanos)
	}

	fn archived(bytes: &StateBytes) -> Result<&Self::Archived, StateError> {
		access_archive::<Self>(bytes)
	}

	unsafe fn archived_trusted(bytes: &StateBytes) -> &Self::Archived {
		// SAFETY: forwarded contract; see OperatorState::archived_trusted.
		unsafe { access_archive_trusted::<Self>(bytes) }
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
	use reifydb_value::value::value_type::ValueType;
	use rkyv::{Archive, Deserialize, Serialize};

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
		let bytes = encode_archive(&value, 7).unwrap();

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
	fn test_row_round_trip_preserves_timestamps() {
		// StateBytes must survive the into_row/from_row boundary it
		// crosses on every store write and read, keeping the row
		// header timestamps that TTL semantics depend on.
		let bytes = encode_archive(&probe(), 1234).unwrap();
		let row = bytes.clone().into_row();
		assert_eq!(row.created_at_nanos(), 1234);

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
		assert!(matches!(err, StateError::UnexpectedShape { .. }));
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
		shape.set_u8(&mut future, 1, 9);
		let err = StateBytes::from_row(future).unwrap_err();
		assert_eq!(err, StateError::UnsupportedFormat(9));
	}

	#[test]
	fn test_truncated_body_fails_validation() {
		// bytecheck must reject a corrupted body as an error rather
		// than panic; this is the disk-corruption trust boundary.
		let bytes = encode_archive(&probe(), 0).unwrap();
		let body = bytes.body();
		let truncated = StateBytes::from_archive(&body[..body.len() / 2], 0);
		assert!(matches!(access_archive::<Probe>(&truncated), Err(StateError::Validation(_))));
	}

	#[test]
	fn test_byte_size_covers_whole_row() {
		let bytes = encode_archive(&probe(), 0).unwrap();
		assert_eq!(bytes.byte_size().as_bytes(), bytes.row().len() as u64);
		assert!(bytes.byte_size().as_bytes() > bytes.body().len() as u64);
	}
}
