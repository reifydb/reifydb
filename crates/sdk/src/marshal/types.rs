// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice::from_raw_parts, str::from_utf8};

use postcard::from_bytes;
use reifydb_abi::data::{buffer::BufferFFI, column::ColumnDataFFI};
use reifydb_value::value::{
	Value,
	blob::Blob,
	container::{
		any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
		identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
		utf8::Utf8Container, uuid::UuidContainer,
	},
	date::Date,
	datetime::DateTime,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	is::IsNumber,
	time::Time,
	uuid::{Uuid4, Uuid7},
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::ffi::arena::Arena;

impl Arena {
	pub(super) fn unmarshal_bool_data(&self, ffi: &ColumnDataFFI) -> BoolContainer {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return BoolContainer::new(vec![false; row_count]);
		}

		unsafe {
			let bytes = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let byte_idx = i / 8;
				let bit_idx = i % 8;
				let val = if byte_idx < bytes.len() {
					(bytes[byte_idx] & (1 << bit_idx)) != 0
				} else {
					false
				};
				values.push(val);
			}
			BoolContainer::new(values)
		}
	}

	pub(super) fn unmarshal_numeric_data<T: Copy + Default + IsNumber>(
		&self,
		ffi: &ColumnDataFFI,
	) -> NumberContainer<T> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return NumberContainer::new(vec![T::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const T;
			let len = ffi.data.len / size_of::<T>();
			let slice = from_raw_parts(ptr, len);
			NumberContainer::new(slice.to_vec())
		}
	}

	pub(super) fn unmarshal_utf8_data(&self, ffi: &ColumnDataFFI) -> Utf8Container {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return Utf8Container::new(vec![String::new(); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut strings = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let s = from_utf8(&data[start..end]).unwrap_or("").to_string();
				strings.push(s);
			}

			Utf8Container::new(strings)
		}
	}

	pub(super) fn unmarshal_date_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<Date> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![Date::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const i32;
			let len = ffi.data.len / size_of::<i32>();
			let slice = from_raw_parts(ptr, len);
			let dates: Vec<Date> = slice
				.iter()
				.map(|&days| Date::from_days_since_epoch(days).unwrap_or_default())
				.collect();
			TemporalContainer::new(dates)
		}
	}

	pub(super) fn unmarshal_datetime_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<DateTime> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![DateTime::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const u64;
			let len = ffi.data.len / size_of::<u64>();
			let slice = from_raw_parts(ptr, len);
			let datetimes: Vec<DateTime> = slice.iter().map(|&nanos| DateTime::from_nanos(nanos)).collect();
			TemporalContainer::new(datetimes)
		}
	}

	pub(super) fn unmarshal_time_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<Time> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![Time::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const u64;
			let len = ffi.data.len / size_of::<u64>();
			let slice = from_raw_parts(ptr, len);
			let times: Vec<Time> = slice
				.iter()
				.map(|&ns| Time::from_nanos_since_midnight(ns).unwrap_or_default())
				.collect();
			TemporalContainer::new(times)
		}
	}

	pub(super) fn unmarshal_duration_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<Duration> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![Duration::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const Duration;
			let len = ffi.data.len / size_of::<Duration>();
			let slice = from_raw_parts(ptr, len);
			TemporalContainer::new(slice.to_vec())
		}
	}

	pub(super) fn unmarshal_identity_id_data(&self, ffi: &ColumnDataFFI) -> IdentityIdContainer {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return IdentityIdContainer::new(vec![IdentityId::default(); row_count]);
		}

		unsafe {
			let bytes = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let ids: Vec<IdentityId> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);

					IdentityId(Uuid7(Uuid::from_bytes(arr)))
				})
				.collect();
			IdentityIdContainer::new(ids)
		}
	}

	pub(super) fn unmarshal_uuid4_data(&self, ffi: &ColumnDataFFI) -> UuidContainer<Uuid4> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return UuidContainer::new(vec![Uuid4::default(); row_count]);
		}

		unsafe {
			let bytes = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let uuids: Vec<Uuid4> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);
					Uuid4(Uuid::from_bytes(arr))
				})
				.collect();
			UuidContainer::new(uuids)
		}
	}

	pub(super) fn unmarshal_uuid7_data(&self, ffi: &ColumnDataFFI) -> UuidContainer<Uuid7> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return UuidContainer::new(vec![Uuid7::default(); row_count]);
		}

		unsafe {
			let bytes = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let uuids: Vec<Uuid7> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);
					Uuid7(Uuid::from_bytes(arr))
				})
				.collect();
			UuidContainer::new(uuids)
		}
	}

	pub(super) fn unmarshal_blob_data(&self, ffi: &ColumnDataFFI) -> BlobContainer {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return BlobContainer::new(vec![Blob::empty(); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut blobs = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				blobs.push(Blob::new(data[start..end].to_vec()));
			}

			BlobContainer::new(blobs)
		}
	}

	pub(super) fn unmarshal_serialized_data<T: Default + Clone + DeserializeOwned + IsNumber>(
		&self,
		ffi: &ColumnDataFFI,
	) -> NumberContainer<T> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return NumberContainer::new(vec![T::default(); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let value: T = from_bytes(&data[start..end]).unwrap_or_default();
				values.push(value);
			}

			NumberContainer::new(values)
		}
	}

	pub(super) fn unmarshal_any_data(&self, ffi: &ColumnDataFFI) -> AnyContainer {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return AnyContainer::new(vec![Box::new(Value::none()); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let value: Value = from_bytes(&data[start..end]).unwrap_or(Value::none());
				values.push(Box::new(value));
			}

			AnyContainer::new(values)
		}
	}

	pub(super) fn unmarshal_dictionary_id_data(&self, ffi: &ColumnDataFFI) -> DictionaryContainer {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return DictionaryContainer::new(vec![DictionaryEntryId::U16(0); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut entries = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let entry: DictionaryEntryId =
					from_bytes(&data[start..end]).unwrap_or(DictionaryEntryId::U16(0));
				entries.push(entry);
			}

			DictionaryContainer::new(entries)
		}
	}

	pub(super) fn read_offsets(&self, ffi: &BufferFFI) -> Vec<u64> {
		if ffi.is_empty() {
			return Vec::new();
		}
		unsafe {
			let ptr = ffi.ptr as *const u64;
			let len = ffi.len / size_of::<u64>();
			from_raw_parts(ptr, len).to_vec()
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;
	use reifydb_value::value::{
		container::temporal::TemporalContainer, date::Date, datetime::DateTime, duration::Duration, time::Time,
	};

	use crate::ffi::arena::Arena;

	// Regression: DateTime columns marshal zero-copy as raw u64 nanos; unmarshal
	// must read them back the same way (not via from_timestamp, which treats the
	// value as epoch seconds).
	#[test]
	fn datetime_column_marshal_unmarshal_roundtrip() {
		let mut arena = Arena::new();
		let values = vec![
			DateTime::from_nanos(0),
			DateTime::from_nanos(1_700_000_000_000_000_000),
			DateTime::from_nanos(u64::MAX),
		];
		let buf = ColumnBuffer::DateTime(TemporalContainer::new(values.clone()));
		let ffi = arena.marshal_column_data(&buf);
		match arena.unmarshal_column_data(&ffi, values.len()) {
			ColumnBuffer::DateTime(container) => {
				let got: &[DateTime] = &container;
				assert_eq!(got, values.as_slice());
			}
			_ => panic!("expected DateTime column"),
		}
	}

	// Regression: Date columns marshal zero-copy as raw i32 days-since-epoch;
	// unmarshal must read them back the same way.
	#[test]
	fn date_column_marshal_unmarshal_roundtrip() {
		let mut arena = Arena::new();
		let values = vec![Date::default(), Date::new(2024, 3, 15).unwrap(), Date::new(1970, 1, 1).unwrap()];
		let buf = ColumnBuffer::Date(TemporalContainer::new(values.clone()));
		let ffi = arena.marshal_column_data(&buf);
		match arena.unmarshal_column_data(&ffi, values.len()) {
			ColumnBuffer::Date(container) => {
				let got: &[Date] = &container;
				assert_eq!(got, values.as_slice());
			}
			_ => panic!("expected Date column"),
		}
	}

	// Regression: Time columns marshal zero-copy as raw u64 nanos-since-midnight;
	// unmarshal must read them back the same way.
	#[test]
	fn time_column_marshal_unmarshal_roundtrip() {
		let mut arena = Arena::new();
		let values = vec![
			Time::default(),
			Time::new(14, 30, 45, 123_456_789).unwrap(),
			Time::new(23, 59, 59, 999_999_999).unwrap(),
		];
		let buf = ColumnBuffer::Time(TemporalContainer::new(values.clone()));
		let ffi = arena.marshal_column_data(&buf);
		match arena.unmarshal_column_data(&ffi, values.len()) {
			ColumnBuffer::Time(container) => {
				let got: &[Time] = &container;
				assert_eq!(got, values.as_slice());
			}
			_ => panic!("expected Time column"),
		}
	}

	// Regression: Duration columns marshal zero-copy as raw 16-byte structs;
	// unmarshal must read them back the same way (not via postcard + offsets,
	// which the zero-copy marshal does not produce).
	#[test]
	fn duration_column_marshal_unmarshal_roundtrip() {
		let mut arena = Arena::new();
		let values = vec![
			Duration::default(),
			Duration::new(13, 5, 3_600_000_000_000).expect("duration"),
			Duration::from_seconds(-30).expect("duration"),
		];
		let buf = ColumnBuffer::Duration(TemporalContainer::new(values.clone()));
		let ffi = arena.marshal_column_data(&buf);
		match arena.unmarshal_column_data(&ffi, values.len()) {
			ColumnBuffer::Duration(container) => {
				let got: &[Duration] = &container;
				assert_eq!(got, values.as_slice());
			}
			_ => panic!("expected Duration column"),
		}
	}
}
