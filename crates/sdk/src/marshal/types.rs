// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Type-specific unmarshalling functions

use std::{slice::from_raw_parts, str::from_utf8};

use postcard::from_bytes;
use reifydb_abi::data::{buffer::BufferFFI, column::ColumnDataFFI};
use reifydb_type::value::{
	Value,
	blob::Blob,
	container::{
		any::AnyContainer, blob::BlobContainer, bool::BoolContainer, identity_id::IdentityIdContainer,
		number::NumberContainer, temporal::TemporalContainer, utf8::Utf8Container, uuid::UuidContainer,
	},
	date::Date,
	datetime::DateTime,
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

	/// Unmarshal numeric data
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

	/// Unmarshal UTF8 data with offsets
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

	/// Unmarshal date data
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

	/// Unmarshal datetime data
	pub(super) fn unmarshal_datetime_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<DateTime> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![DateTime::default(); row_count]);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const i64;
			let len = ffi.data.len / size_of::<i64>();
			let slice = from_raw_parts(ptr, len);
			let datetimes: Vec<DateTime> =
				slice.iter().map(|&ts| DateTime::from_timestamp(ts).unwrap_or_default()).collect();
			TemporalContainer::new(datetimes)
		}
	}

	/// Unmarshal time data
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

	/// Unmarshal duration data (deserialize with postcard since Duration has 3 fields)
	pub(super) fn unmarshal_duration_data(&self, ffi: &ColumnDataFFI) -> TemporalContainer<Duration> {
		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return TemporalContainer::new(vec![Duration::default(); row_count]);
		}

		unsafe {
			let data = from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut durations = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let duration: Duration = from_bytes(&data[start..end]).unwrap_or_default();
				durations.push(duration);
			}

			TemporalContainer::new(durations)
		}
	}

	/// Unmarshal identity ID data
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
					// IdentityId wraps Uuid7 which wraps StdUuid
					IdentityId(Uuid7(Uuid::from_bytes(arr)))
				})
				.collect();
			IdentityIdContainer::new(ids)
		}
	}

	/// Unmarshal UUID4 data
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

	/// Unmarshal UUID7 data
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

	/// Unmarshal blob data with offsets
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

	/// Unmarshal serialized data with offsets
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

	/// Unmarshal Any data with offsets
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
				let value: Value = postcard::from_bytes(&data[start..end]).unwrap_or(Value::none());
				values.push(Box::new(value));
			}

			AnyContainer::new(values)
		}
	}

	/// Helper: read offsets array from FFI buffer
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
